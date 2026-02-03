mod codec;
mod db;
mod error;

pub use crate::codec::{Compressor, DataEntry, EntryType, Lz77};
pub use crate::db::{
    append_entry, DatabaseConfig, IndexEntry, LogIter, LogRecord, MyDatabase, SharedState,
};
pub use crate::error::DatabaseError;
