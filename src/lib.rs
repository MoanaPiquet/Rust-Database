use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

/// Configuration de la base de données
#[derive(Debug)]
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

/// Représentation d'une donnée à écrire
pub struct DataEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl DataEntry {
    /// Sérialise l'entrée en format binaire :
    /// [Taille Clé (4B)] [Taille Valeur (4B)] [Clé] [Valeur] [Checksum (4B)]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buffer = Vec::new();

        let key_len = (self.key.len() as u32).to_be_bytes();
        let val_len = (self.value.len() as u32).to_be_bytes();

        buffer.extend_from_slice(&key_len);
        buffer.extend_from_slice(&val_len);
        buffer.extend_from_slice(&self.key);
        buffer.extend_from_slice(&self.value);

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

pub struct IndexEntry {
    pub offset: u64,
    pub size: u32,
}

pub struct MyDatabase {
    pub config: DatabaseConfig,
    pub index: HashMap<Vec<u8>, IndexEntry>,
}

impl MyDatabase {
    pub fn new(config: DatabaseConfig) -> Self {
        Self {
            config,
            index: HashMap::new(),
        }
    }

    /// Écrit une donnée et met à jour l'index
    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> io::Result<()> {
        let entry = DataEntry {
            key: key.clone(),
            value,
        };
        let bytes = entry.to_bytes();
        let size = bytes.len() as u32;

        let mut file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&self.config.file_path)?;

        let offset = file.seek(SeekFrom::End(0))?;

        file.write_all(&bytes)?;

        self.index.insert(key, IndexEntry { offset, size });
        Ok(())
    }

    /// Lit une donnée en utilisant l'index et valide la clé
    pub fn get(&mut self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let index_info = match self.index.get(key) {
            Some(e) => e,
            None => return Ok(None),
        };

        let mut file = File::open(&self.config.file_path)?;
        file.seek(SeekFrom::Start(index_info.offset))?;

        let mut buffer = vec![0; index_info.size as usize];
        file.read_exact(&mut buffer)?;

        // Extraire les tailles sans unwrap
        let key_len_bytes: [u8; 4] = buffer[0..4].try_into().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "Impossible de lire la taille de la clé",
            )
        })?;
        let key_len = u32::from_be_bytes(key_len_bytes) as usize;

        let value_len_bytes: [u8; 4] = buffer[4..8].try_into().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "Impossible de lire la taille de la valeur",
            )
        })?;
        let value_len = u32::from_be_bytes(value_len_bytes) as usize;

        // Vérifier les bounds
        if 8 + key_len + value_len + 4 > buffer.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Données corrompues : tailles invalides",
            ));
        }

        let key_start = 8;
        let key_end = key_start + key_len;
        let value_start = key_end;
        let value_end = value_start + value_len;
        let checksum_start = value_end;

        // Valider que la clé lue correspond à la clé demandée
        if &buffer[key_start..key_end] != key {
            return Ok(None);
        }

        // Vérifier le checksum
        let mut somme: u32 = 0;
        for byte in &buffer[0..value_end] {
            somme = somme.wrapping_add(*byte as u32);
        }

        let stored_checksum_bytes: [u8; 4] = buffer[checksum_start..checksum_start + 4]
            .try_into()
            .map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "Impossible de lire le checksum")
            })?;
        let stored_checksum = u32::from_be_bytes(stored_checksum_bytes);

        if somme != stored_checksum {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Checksum invalide !",
            ));
        }

        Ok(Some(buffer[value_start..value_end].to_vec()))
    }
}
