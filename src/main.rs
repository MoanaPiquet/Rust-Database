use rust_database::{DatabaseConfig, DatabaseError, MyDatabase};
use std::fs;
use std::io::{self, Write};
use std::path::PathBuf;

/// Point d'entrée CLI (REPL).
fn main() -> Result<(), DatabaseError> {
    let config = DatabaseConfig::new();
    let db = MyDatabase::new(config)?;

    println!("=== Rust Database CLI (REPL) ===");
    println!("Commandes disponibles:");
    println!("  SET <clé> <valeur>  - Ajoute/met à jour une clé");
    println!("  SET <clé> --file <chemin>  - Stocke le contenu d'un fichier");
    println!("  GET <clé>           - Récupère une valeur");
    println!("  GET <clé> --file <chemin>  - Écrit la valeur dans un fichier");
    println!("  DELETE <clé>        - Supprime une clé (Tombstone)");
    println!("  COMPACT             - Compacter le fichier de log");
    println!("  LOG [--limit N]     - Affiche les entrées du journal");
    println!("  EXIT                - Quitte le programme\n");

    loop {
        print!("rdb > ");
        io::stdout().flush().map_err(DatabaseError::Io)?;

        let mut input = String::new();
        io::stdin()
            .read_line(&mut input)
            .map_err(DatabaseError::Io)?;

        let input = input.trim();

        if input.is_empty() {
            continue;
        }

        let parts: Vec<&str> = input.split_whitespace().collect();
        let command = parts[0].to_uppercase();

        match command.as_str() {
            "SET" => {
                if parts.len() < 3 {
                    println!("Usage: SET <clé> <valeur>");
                    println!("   ou: SET <clé> --file <chemin>");
                    continue;
                }

                let key = parts[1].as_bytes().to_vec();
                let value = if parts[2] == "--file" {
                    if parts.len() < 4 {
                        println!("Usage: SET <clé> --file <chemin>");
                        continue;
                    }
                    let path = PathBuf::from(parts[3]);
                    match fs::read(&path) {
                        Ok(bytes) => bytes,
                        Err(e) => {
                            println!("Erreur lecture fichier: {}", e);
                            continue;
                        }
                    }
                } else {
                    parts[2..].join(" ").as_bytes().to_vec()
                };

                let is_file = parts[2] == "--file";
                match db.set(key.clone(), value.clone()) {
                    Ok(_) if is_file => {
                        println!("SET '{}' = <{} octets>", display_bytes(&key), value.len())
                    }
                    Ok(_) => println!(
                        "SET '{}' = '{}'",
                        display_bytes(&key),
                        display_bytes(&value)
                    ),
                    Err(e) => println!("Erreur SET: {}", e),
                }
            }

            "GET" => {
                if parts.len() < 2 {
                    println!("Usage: GET <clé>");
                    println!("   ou: GET <clé> --file <chemin>");
                    continue;
                }

                let key = parts[1].as_bytes().to_vec();

                match db.get(&key) {
                    Ok(Some(value)) => {
                        if parts.len() >= 4 && parts[2] == "--file" {
                            let path = PathBuf::from(parts[3]);
                            match fs::write(&path, &value) {
                                Ok(_) => println!(
                                    "GET '{}' -> fichier écrit: {}",
                                    display_bytes(&key),
                                    path.display()
                                ),
                                Err(e) => println!("Erreur écriture fichier: {}", e),
                            }
                        } else {
                            println!(
                                "GET '{}' = '{}'",
                                display_bytes(&key),
                                display_bytes(&value)
                            );
                        }
                    }
                    Ok(None) => println!("Clé '{}' non trouvée", display_bytes(&key)),
                    Err(e) => println!("Erreur GET: {}", e),
                }
            }

            "DELETE" => {
                if parts.len() < 2 {
                    println!("Usage: DELETE <clé>");
                    continue;
                }

                let key = parts[1].as_bytes().to_vec();

                match db.delete(key.clone()) {
                    Ok(_) => println!("DELETE '{}' (Tombstone écrit)", display_bytes(&key)),
                    Err(e) => println!("Erreur DELETE: {}", e),
                }
            }

            "EXIT" | "QUIT" => {
                println!("Fermeture de la base de données...");
                break;
            }

            "COMPACT" => match db.compact() {
                Ok(_) => println!("Compaction terminée, log réduit."),
                Err(e) => println!("Erreur COMPACT: {}", e),
            },

            "LOG" => {
                let limit = if parts.len() >= 3 && parts[1] == "--limit" {
                    parts[2].parse::<usize>().ok()
                } else {
                    None
                };

                match db.log_iter() {
                    Ok(iter) => {
                        for (idx, record) in iter.flatten().enumerate() {
                            if let Some(max) = limit
                                && idx >= max
                            {
                                break;
                            }
                            let entry_type = match record.entry_type {
                                rust_database::EntryType::Data => "DATA",
                                rust_database::EntryType::Tombstone => "TOMBSTONE",
                            };
                            println!(
                                "#{idx} offset={} size={} type={} key={} checksum_ok={}",
                                record.offset,
                                record.size,
                                entry_type,
                                display_bytes(&record.key),
                                record.checksum_ok
                            );
                        }
                    }
                    Err(e) => println!("Erreur LOG: {}", e),
                }
            }

            _ => {
                println!("Commande inconnue. Commandes disponibles :");
                println!("  SET <clé> <valeur> : Enregistrer une donnée");
                println!("  SET <clé> --file <chemin> : Enregistrer un fichier");
                println!("  GET <clé>          : Lire une donnée");
                println!("  GET <clé> --file <chemin> : Écrire une donnée en fichier");
                println!("  DELETE <clé>       : Supprimer une donnée");
                println!("  COMPACT            : Réduire le fichier de log");
                println!("  LOG [--limit N]    : Voir le le fichier de log");
                println!("  EXIT               : Quitter le programme");
            }
        }
    }

    Ok(())
}

/// Affiche une valeur UTF-8 ou un hex en fallback.
fn display_bytes(bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(text) => text.to_string(),
        Err(_) => {
            let mut out = String::with_capacity(bytes.len() * 2);
            for b in bytes {
                out.push_str(&format!("{:02x}", b));
            }
            format!("0x{}", out)
        }
    }
}
