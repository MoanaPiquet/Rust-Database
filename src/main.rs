use rust_database::{DatabaseConfig, DatabaseError, MyDatabase};
use std::fs;
use std::io::{self, Write};
use std::path::PathBuf;

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
                    Ok(_) if is_file => println!(
                        "SET '{}' = <{} octets>",
                        String::from_utf8(key)?,
                        value.len()
                    ),
                    Ok(_) => println!(
                        "SET '{}' = '{}'",
                        String::from_utf8(key)?,
                        String::from_utf8(value)?
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
                                    String::from_utf8(key.clone())?,
                                    path.display()
                                ),
                                Err(e) => println!("Erreur écriture fichier: {}", e),
                            }
                        } else {
                            println!(
                                "GET '{}' = '{}'",
                                String::from_utf8(key.clone())?,
                                String::from_utf8(value)?
                            );
                        }
                    }
                    Ok(None) => println!("Clé '{}' non trouvée", String::from_utf8(key)?),
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
                    Ok(_) => println!("DELETE '{}' (Tombstone écrit)", String::from_utf8(key)?),
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

            _ => {
                println!("Commande inconnue. Commandes disponibles :");
                println!("  SET <clé> <valeur> : Enregistrer une donnée");
                println!("  SET <clé> --file <chemin> : Enregistrer un fichier");
                println!("  GET <clé>          : Lire une donnée");
                println!("  GET <clé> --file <chemin> : Écrire une donnée en fichier");
                println!("  DELETE <clé>       : Supprimer une donnée");
                println!("  COMPACT            : Réduire le fichier de log");
                println!("  EXIT               : Quitter le programme");
            }
        }
    }

    Ok(())
}
