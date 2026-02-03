use std::fmt;
use std::io;

/// Erreurs métier de la base.
#[derive(Debug)]
pub enum DatabaseError {
    Io(io::Error),
    CorruptedData,
    InvalidFormat,
    KeyNotFound(String),
    ParseError(String),
    Utf8(std::string::FromUtf8Error),
    LockPoisoned(&'static str),
}

impl fmt::Display for DatabaseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
