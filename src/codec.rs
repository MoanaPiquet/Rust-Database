use crate::error::DatabaseError;

/// Compression générique pour encoder/décoder des octets.
pub trait Compressor {
    fn encode(input: &[u8]) -> Vec<u8>;
    fn decode(input: &[u8]) -> Result<Vec<u8>, DatabaseError>;
}

#[derive(Debug, Clone, Copy)]
/// Type d'entrée dans le journal.
pub enum EntryType {
    Data,
    Tombstone,
}

/// Entrée logique du journal (clé/valeur).
pub struct DataEntry {
    pub entry_type: EntryType,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl DataEntry {
    /// Sérialise une entrée en format binaire.
    /// \[Type (1B)\] \[Taille Clé (4B)\] \[Taille Valeur (4B)\] \[Clé\] \[Valeur\] \[Checksum (4B)\]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buffer = Vec::new();

        let type_byte = match self.entry_type {
            EntryType::Data => 0u8,
            EntryType::Tombstone => 1u8,
        };
        buffer.push(type_byte);

        let key_len = (self.key.len() as u32).to_be_bytes();
        let encoded_value = Lz77::encode(&self.value);
        let val_len = (encoded_value.len() as u32).to_be_bytes();

        buffer.extend_from_slice(&key_len);
        buffer.extend_from_slice(&val_len);
        buffer.extend_from_slice(&self.key);
        buffer.extend_from_slice(&encoded_value);

        let mut checksum: u32 = 0;
        for byte in &buffer {
            checksum = checksum.wrapping_add(*byte as u32);
        }
        buffer.extend_from_slice(&checksum.to_be_bytes());

        buffer
    }
}

/// Implémentation LZ77 simplifiée.
pub struct Lz77;

impl Compressor for Lz77 {
    fn encode(input: &[u8]) -> Vec<u8> {
        lz77_encode(input)
    }

    fn decode(input: &[u8]) -> Result<Vec<u8>, DatabaseError> {
        lz77_decode(input)
    }
}

fn lz77_encode(input: &[u8]) -> Vec<u8> {
    if input.is_empty() {
        return Vec::new();
    }

    let mut out = Vec::new();
    let mut literals: Vec<u8> = Vec::new();
    let mut i = 0;

    while i < input.len() {
        let (dist, len) = find_longest_match(input, i);
        if len >= 3 {
            if !literals.is_empty() {
                emit_literals(&mut out, &mut literals);
            }
            out.push(1);
            out.extend_from_slice(&(dist as u16).to_be_bytes());
            out.push(len as u8);
            i += len;
        } else {
            literals.push(input[i]);
            if literals.len() == u8::MAX as usize {
                emit_literals(&mut out, &mut literals);
            }
            i += 1;
        }
    }

    if !literals.is_empty() {
        emit_literals(&mut out, &mut literals);
    }

    out
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
