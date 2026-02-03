use crate::codec::{Compressor, DataEntry, EntryType, Lz77};
use crate::error::DatabaseError;
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
            max_size: 1024 * 1024,
        }
    }
}

impl DatabaseConfig {
    pub fn new() -> Self {
        Self::default()
    }
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

/// Moteur principal de la base clé/valeur.
pub struct MyDatabase {
    pub config: DatabaseConfig,
    pub shared: Arc<SharedState>,
}

pub struct LogRecord {
    pub offset: u64,
    pub size: u32,
    pub entry_type: EntryType,
    pub key: Vec<u8>,
    pub value_len: usize,
    pub checksum_ok: bool,
}

/// Itérateur public sur le journal.
pub struct LogIter {
    reader: LogReader,
}

struct LogReader {
    file: File,
    offset: u64,
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

impl MyDatabase {
    /// Ouvre la base et reconstruit l'index au démarrage.
    pub fn new(config: DatabaseConfig) -> Result<Self, DatabaseError> {
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&config.file_path)?;

        let index = Self::recover_index(&config.file_path)?;
        let shared = Arc::new(SharedState {
            file: Mutex::new(file),
            access: RwLock::new(()),
            index: RwLock::new(index),
        });

        Ok(Self { config, shared })
    }

    /// Ajoute ou met à jour une valeur.
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

    /// Récupère une valeur si elle existe.
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

    /// Supprime une clé via tombstone.
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

        let key_len = u32::from_be_bytes(
            buffer[1..5]
                .try_into()
                .map_err(|_| DatabaseError::InvalidFormat)?,
        ) as usize;
        let value_len = u32::from_be_bytes(
            buffer[5..9]
                .try_into()
                .map_err(|_| DatabaseError::InvalidFormat)?,
        ) as usize;

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

        let stored_checksum = u32::from_be_bytes(
            buffer[checksum_start..checksum_start + 4]
                .try_into()
                .map_err(|_| DatabaseError::CorruptedData)?,
        );
        if somme != stored_checksum {
            return Err(DatabaseError::CorruptedData);
        }

        let decoded = Lz77::decode(&buffer[value_start..value_end])?;
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

    /// Compacte le journal pour ne garder que les entrées valides.
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

    fn recover_index(path: &PathBuf) -> Result<HashMap<Vec<u8>, IndexEntry>, DatabaseError> {
        let mut index = HashMap::new();
        let mut iter = LogIter::new(path)?;

        for record in &mut iter {
            let record = record?;
            if !record.checksum_ok {
                return Err(DatabaseError::CorruptedData);
            }
            index.insert(
                record.key,
                IndexEntry {
                    offset: record.offset,
                    size: record.size,
                },
            );
        }

        Ok(index)
    }

    /// Retourne un itérateur sur le journal (lecture seule).
    pub fn log_iter(&self) -> Result<LogIter, DatabaseError> {
        LogIter::new(&self.config.file_path)
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

impl LogReader {
    fn new(path: &PathBuf) -> Result<Self, DatabaseError> {
        Ok(Self {
            file: File::open(path)?,
            offset: 0,
        })
    }
}

impl LogIter {
    fn new(path: &PathBuf) -> Result<Self, DatabaseError> {
        Ok(Self {
            reader: LogReader::new(path)?,
        })
    }
}

impl Iterator for LogIter {
    type Item = Result<LogRecord, DatabaseError>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut header = [0u8; 9];
        match self.reader.file.read_exact(&mut header) {
            Ok(_) => {}
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => return None,
            Err(err) => return Some(Err(err.into())),
        }

        let entry_type = match header[0] {
            0 => EntryType::Data,
            1 => EntryType::Tombstone,
            _ => return Some(Err(DatabaseError::InvalidFormat)),
        };

        let key_len = u32::from_be_bytes(
            header[1..5]
                .try_into()
                .map_err(|_| DatabaseError::InvalidFormat)?,
        ) as usize;
        let value_len = u32::from_be_bytes(
            header[5..9]
                .try_into()
                .map_err(|_| DatabaseError::InvalidFormat)?,
        ) as usize;
        let total_size = 9usize + key_len + value_len + 4usize;

        let mut body = vec![0u8; key_len + value_len + 4];
        if let Err(err) = self.reader.file.read_exact(&mut body) {
            if err.kind() == io::ErrorKind::UnexpectedEof {
                return None;
            }
            return Some(Err(err.into()));
        }

        let checksum_start = key_len + value_len;
        let stored_checksum = u32::from_be_bytes(
            body[checksum_start..checksum_start + 4]
                .try_into()
                .map_err(|_| DatabaseError::CorruptedData)?,
        );

        let mut sum: u32 = 0;
        for byte in &header {
            sum = sum.wrapping_add(*byte as u32);
        }
        for byte in &body[..checksum_start] {
            sum = sum.wrapping_add(*byte as u32);
        }

        let checksum_ok = sum == stored_checksum;
        let key = body[..key_len].to_vec();
        let record = LogRecord {
            offset: self.reader.offset,
            size: total_size as u32,
            entry_type,
            key,
            value_len,
            checksum_ok,
        };
        self.reader.offset += total_size as u64;
        Some(Ok(record))
    }
}
