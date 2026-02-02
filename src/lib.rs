use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};

/// Configuration de la base de données
#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    pub file_path: PathBuf,
    pub max_size: u64,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            file_path: PathBuf::from("database.db"),
            max_size: 1024 * 1024, // 1 Mo
        }
    }
}

impl DatabaseConfig {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Debug, Clone, Copy)]
pub enum EntryType {
    Data,
    Tombstone,
}

pub struct DataEntry {
    pub entry_type: EntryType,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl DataEntry {
    // Sérialise l'entrée en format binaire :
    // [Type (1B)] [Taille Clé (4B)] [Taille Valeur (4B)] [Clé] [Valeur] [Checksum (4B)]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buffer = Vec::new();

        let type_byte = match self.entry_type {
            EntryType::Data => 0u8,
            EntryType::Tombstone => 1u8,
        };
        buffer.push(type_byte);

        let key_len = (self.key.len() as u32).to_be_bytes();
        let encoded_value = MyDatabase::lz77_encode(&self.value);
        let val_len = (encoded_value.len() as u32).to_be_bytes();

        buffer.extend_from_slice(&key_len);
        buffer.extend_from_slice(&val_len);
        buffer.extend_from_slice(&self.key);
        buffer.extend_from_slice(&encoded_value);

        // Somme de contrôle simple
        let mut checksum: u32 = 0;
        for byte in &buffer {
            checksum = checksum.wrapping_add(*byte as u32);
        }
        buffer.extend_from_slice(&checksum.to_be_bytes());

        buffer
    }
}

/// Ajoute une entrée à la fin du fichier (Append-only)
pub fn append_entry(config: &DatabaseConfig, entry: &DataEntry) -> io::Result<()> {
    let mut file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(&config.file_path)?;

    let bytes = entry.to_bytes();
    file.write_all(&bytes)?;
    file.flush()?;
    Ok(())
}

#[derive(Clone, Copy)]
pub struct IndexEntry {
    pub offset: u64,
    pub size: u32,
}

pub struct SharedState {
    pub file: Mutex<File>,
    pub access: RwLock<()>,
    pub index: RwLock<HashMap<Vec<u8>, IndexEntry>>,
}

pub struct MyDatabase {
    pub config: DatabaseConfig,
    pub shared: Arc<SharedState>,
}

#[derive(Debug)]
pub enum DatabaseError {
    Io(std::io::Error),
    CorruptedData,
    InvalidFormat,
    KeyNotFound(String),
    ParseError(String),
    Utf8(std::string::FromUtf8Error),
    LockPoisoned(&'static str),
}

impl std::fmt::Display for DatabaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DatabaseError::Io(err) => write!(f, "Erreur E/S système : {}", err),
            DatabaseError::CorruptedData => {
                write!(f, "Données corrompues : le checksum ne correspond pas")
            }
            DatabaseError::InvalidFormat => write!(f, "Format de fichier invalide ou incompatible"),
            DatabaseError::KeyNotFound(key) => write!(f, "Clé non trouvée : '{}'", key),
            DatabaseError::ParseError(msg) => write!(f, "Erreur de commande : {}", msg),
            DatabaseError::Utf8(err) => write!(f, "Données corrompues (UTF-8) : {}", err),
            DatabaseError::LockPoisoned(resource) => {
                write!(f, "Verrouillage indisponible : {}", resource)
            }
        }
    }
}

impl From<io::Error> for DatabaseError {
    fn from(err: io::Error) -> Self {
        DatabaseError::Io(err)
    }
}

impl From<std::string::FromUtf8Error> for DatabaseError {
    fn from(err: std::string::FromUtf8Error) -> Self {
        DatabaseError::Utf8(err)
    }
}

impl std::error::Error for DatabaseError {}

impl MyDatabase {
    pub fn new(config: DatabaseConfig) -> Result<Self, DatabaseError> {
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&config.file_path)?;

        let shared = Arc::new(SharedState {
            file: Mutex::new(file),
            access: RwLock::new(()),
            index: RwLock::new(HashMap::new()),
        });

        Ok(Self { config, shared })
    }

    pub fn set(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), DatabaseError> {
        let entry = DataEntry {
            entry_type: EntryType::Data,
            key: key.clone(),
            value,
        };
        let bytes = entry.to_bytes();
        let size = bytes.len() as u32;

        {
            let _access_guard = self
                .shared
                .access
                .write()
                .map_err(|_| DatabaseError::LockPoisoned("lecteur/rédacteur"))?;

            let offset = {
                let mut file = self
                    .shared
                    .file
                    .lock()
                    .map_err(|_| DatabaseError::LockPoisoned("fichier"))?;
                let offset = file.seek(SeekFrom::End(0))?;
                file.write_all(&bytes)?;
                file.flush()?;
                offset
            };

            let mut index = self
                .shared
                .index
                .write()
                .map_err(|_| DatabaseError::LockPoisoned("index"))?;
            index.insert(key.clone(), IndexEntry { offset, size });
        }

        self.maybe_compact()?;
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, DatabaseError> {
        let index_info = {
            let index = self
                .shared
                .index
                .read()
                .map_err(|_| DatabaseError::LockPoisoned("index"))?;
            match index.get(key) {
                Some(entry) => *entry,
                None => return Ok(None),
            }
        };

        let _access_guard = self
            .shared
            .access
            .read()
            .map_err(|_| DatabaseError::LockPoisoned("lecteur/rédacteur"))?;

        let mut file = File::open(&self.config.file_path)?;
        Self::read_entry_value(&mut file, &index_info, key)
    }

    pub fn delete(&self, key: Vec<u8>) -> Result<(), DatabaseError> {
        let entry = DataEntry {
            entry_type: EntryType::Tombstone,
            key: key.clone(),
            value: Vec::new(),
        };
        let bytes = entry.to_bytes();
        let size = bytes.len() as u32;

        {
            let _access_guard = self
                .shared
                .access
                .write()
                .map_err(|_| DatabaseError::LockPoisoned("lecteur/rédacteur"))?;

            let offset = {
                let mut file = self
                    .shared
                    .file
                    .lock()
                    .map_err(|_| DatabaseError::LockPoisoned("fichier"))?;
                let offset = file.seek(SeekFrom::End(0))?;
                file.write_all(&bytes)?;
                file.flush()?;
                offset
            };

            let mut index = self
                .shared
                .index
                .write()
                .map_err(|_| DatabaseError::LockPoisoned("index"))?;
            index.insert(key.clone(), IndexEntry { offset, size });
        }

        self.maybe_compact()?;
        Ok(())
    }

    fn decode_buffer(buffer: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>, DatabaseError> {
        if buffer.len() < 9 {
            return Err(DatabaseError::InvalidFormat);
        }

        if buffer[0] == 1 {
            return Ok(None);
        }

        let key_len_bytes: [u8; 4] = buffer[1..5]
            .try_into()
            .map_err(|_| DatabaseError::InvalidFormat)?;
        let key_len = u32::from_be_bytes(key_len_bytes) as usize;

        let value_len_bytes: [u8; 4] = buffer[5..9]
            .try_into()
            .map_err(|_| DatabaseError::InvalidFormat)?;
        let value_len = u32::from_be_bytes(value_len_bytes) as usize;

        let total_len = 9 + key_len + value_len + 4;
        if total_len > buffer.len() {
            return Err(DatabaseError::CorruptedData);
        }

        let key_start = 9;
        let key_end = key_start + key_len;
        let value_start = key_end;
        let value_end = value_start + value_len;
        let checksum_start = value_end;

        if &buffer[key_start..key_end] != key {
            return Ok(None);
        }

        let mut somme: u32 = 0;
        for byte in &buffer[0..value_end] {
            somme = somme.wrapping_add(*byte as u32);
        }

        let stored_checksum_bytes: [u8; 4] = buffer[checksum_start..checksum_start + 4]
            .try_into()
            .map_err(|_| DatabaseError::InvalidFormat)?;
        let stored_checksum = u32::from_be_bytes(stored_checksum_bytes);

        if somme != stored_checksum {
            return Err(DatabaseError::CorruptedData);
        }

        let decoded = Self::lz77_decode(&buffer[value_start..value_end])?;
        Ok(Some(decoded))
    }

    fn read_entry_value(
        reader: &mut File,
        entry: &IndexEntry,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, DatabaseError> {
        let mut buffer = vec![0; entry.size as usize];
        reader.seek(SeekFrom::Start(entry.offset))?;
        reader.read_exact(&mut buffer)?;
        Self::decode_buffer(&buffer, key)
    }

    fn lz77_encode(input: &[u8]) -> Vec<u8> {
        if input.is_empty() {
            return Vec::new();
        }

        let mut out = Vec::new();
        let mut literals: Vec<u8> = Vec::new();
        let mut i = 0;

        while i < input.len() {
            let (dist, len) = Self::find_longest_match(input, i);
            if len >= 3 {
                if !literals.is_empty() {
                    Self::emit_literals(&mut out, &mut literals);
                }
                out.push(1);
                out.extend_from_slice(&(dist as u16).to_be_bytes());
                out.push(len as u8);
                i += len;
            } else {
                literals.push(input[i]);
                if literals.len() == u8::MAX as usize {
                    Self::emit_literals(&mut out, &mut literals);
                }
                i += 1;
            }
        }

        if !literals.is_empty() {
            Self::emit_literals(&mut out, &mut literals);
        }

        out
    }

    fn emit_literals(out: &mut Vec<u8>, literals: &mut Vec<u8>) {
        out.push(0);
        out.push(literals.len() as u8);
        out.extend_from_slice(literals);
        literals.clear();
    }

    fn find_longest_match(input: &[u8], pos: usize) -> (usize, usize) {
        let window = 4095usize;
        let max_len = 255usize;
        let start = pos.saturating_sub(window);
        let mut best_len = 0usize;
        let mut best_dist = 0usize;

        for j in start..pos {
            let mut len = 0usize;
            while len < max_len && pos + len < input.len() && input[j + len] == input[pos + len] {
                len += 1;
            }

            if len > best_len {
                best_len = len;
                best_dist = pos - j;
                if best_len == max_len {
                    break;
                }
            }
        }

        (best_dist, best_len)
    }

    fn lz77_decode(input: &[u8]) -> Result<Vec<u8>, DatabaseError> {
        if input.is_empty() {
            return Ok(Vec::new());
        }

        let mut out = Vec::new();
        let mut i = 0usize;
        while i < input.len() {
            let tag = input[i];
            i += 1;

            match tag {
                0 => {
                    if i >= input.len() {
                        return Err(DatabaseError::InvalidFormat);
                    }
                    let len = input[i] as usize;
                    i += 1;
                    if len == 0 || i + len > input.len() {
                        return Err(DatabaseError::InvalidFormat);
                    }
                    out.extend_from_slice(&input[i..i + len]);
                    i += len;
                }
                1 => {
                    if i + 2 >= input.len() {
                        return Err(DatabaseError::InvalidFormat);
                    }
                    let dist = u16::from_be_bytes([input[i], input[i + 1]]) as usize;
                    i += 2;
                    let len = input[i] as usize;
                    i += 1;
                    if dist == 0 || len == 0 || dist > out.len() {
                        return Err(DatabaseError::InvalidFormat);
                    }
                    for _ in 0..len {
                        let b = out[out.len() - dist];
                        out.push(b);
                    }
                }
                _ => return Err(DatabaseError::InvalidFormat),
            }
        }

        Ok(out)
    }

    pub fn compact(&self) -> Result<(), DatabaseError> {
        let _access_guard = self
            .shared
            .access
            .write()
            .map_err(|_| DatabaseError::LockPoisoned("lecteur/rédacteur"))?;

        let index_snapshot = {
            let index = self
                .shared
                .index
                .read()
                .map_err(|_| DatabaseError::LockPoisoned("index"))?;
            let mut snapshot: Vec<(Vec<u8>, IndexEntry)> =
                index.iter().map(|(k, entry)| (k.clone(), *entry)).collect();
            snapshot.sort_by_key(|(_, entry)| entry.offset);
            snapshot
        };

        let live_entries = {
            let mut reader = File::open(&self.config.file_path)?;
            let mut entries = Vec::new();
            for (key, entry) in index_snapshot {
                if let Some(value) = Self::read_entry_value(&mut reader, &entry, &key)? {
                    entries.push((key, value));
                }
            }
            entries
        };

        let temp_path = self.config.file_path.with_extension("db.compacted");
        let _ = std::fs::remove_file(&temp_path);

        let mut new_index = HashMap::new();
        {
            let mut temp_file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&temp_path)?;
            for (key, value) in &live_entries {
                let entry = DataEntry {
                    entry_type: EntryType::Data,
                    key: key.clone(),
                    value: value.clone(),
                };
                let bytes = entry.to_bytes();
                let offset = temp_file.seek(SeekFrom::End(0))?;
                temp_file.write_all(&bytes)?;
                new_index.insert(
                    key.clone(),
                    IndexEntry {
                        offset,
                        size: bytes.len() as u32,
                    },
                );
            }
            temp_file.flush()?;
        }

        {
            let _guard = self
                .shared
                .file
                .lock()
                .map_err(|_| DatabaseError::LockPoisoned("fichier"))?;
            drop(_guard);
        }

        match std::fs::rename(&temp_path, &self.config.file_path) {
            Ok(_) => {}
            Err(err) if err.kind() == ErrorKind::AlreadyExists => {
                std::fs::remove_file(&self.config.file_path)?;
                std::fs::rename(&temp_path, &self.config.file_path)?;
            }
            Err(err) => return Err(err.into()),
        }

        let new_file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&self.config.file_path)?;
        {
            let mut guard = self
                .shared
                .file
                .lock()
                .map_err(|_| DatabaseError::LockPoisoned("fichier"))?;
            *guard = new_file;
        }

        let mut index_guard = self
            .shared
            .index
            .write()
            .map_err(|_| DatabaseError::LockPoisoned("index"))?;
        *index_guard = new_index;

        Ok(())
    }

    fn file_size(&self) -> Result<u64, DatabaseError> {
        Ok(std::fs::metadata(&self.config.file_path)?.len())
    }

    fn maybe_compact(&self) -> Result<(), DatabaseError> {
        if self.config.max_size == 0 {
            return Ok(());
        }

        loop {
            let len = self.file_size()?;
            if len < self.config.max_size {
                break;
            }
            let before = len;
            self.compact()?;
            let after = self.file_size()?;
            if after >= before {
                break;
            }
        }

        Ok(())
    }
}

impl Clone for MyDatabase {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            shared: Arc::clone(&self.shared),
        }
    }
}
