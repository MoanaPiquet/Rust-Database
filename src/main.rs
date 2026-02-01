use rust_database::{DatabaseConfig, DatabaseError, MyDatabase};
use std::io::{self, Write};

fn main() -> Result<(), DatabaseError> {
    let config = DatabaseConfig::new();
    let db = MyDatabase::new(config)?;

    println!("=== Rust Database CLI (REPL) ===");
    println!("Commandes disponibles:");
    println!("  SET <clé> <valeur>  - Ajoute/met à jour une clé");
    println!("  GET <clé>           - Récupère une valeur");
    println!("  DELETE <clé>        - Supprime une clé (Tombstone)");
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
                    continue;
                }

                let key = parts[1].as_bytes().to_vec();
                let value = parts[2..].join(" ").as_bytes().to_vec();

                match db.set(key.clone(), value.clone()) {
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
                    continue;
                }

                let key = parts[1].as_bytes().to_vec();

                match db.get(&key) {
                    Ok(Some(value)) => println!(
                        "GET '{}' = '{}'",
                        String::from_utf8(key.clone())?,
                        String::from_utf8(value)?
                    ),
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

            _ => {
                println!("Commande inconnue. Commandes disponibles :");
                println!("  SET <clé> <valeur> : Enregistrer une donnée");
                println!("  GET <clé>          : Lire une donnée");
                println!("  DELETE <clé>       : Supprimer une donnée");
                println!("  EXIT               : Quitter le programme");
            }
        }
    }

    Ok(())
}
