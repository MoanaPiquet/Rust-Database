use rust_database::{DatabaseConfig, MyDatabase};

fn main() -> Result<(), std::io::Error> {
    let config = DatabaseConfig::new();
    println!("Démarrage avec la config : {:?}", config);

    let mut db = MyDatabase::new(config);

    let key = b"nom".to_vec();
    let value = b"rustacean".to_vec();
    let _ = db.put(key.clone(), value.clone());
    println!(
        "✓ Donnée écrite : {:?} = {:?}",
        String::from_utf8_lossy(&key),
        String::from_utf8_lossy(&value)
    );

    match db.get(&key)? {
        Some(retrieved_value) => {
            println!(
                "✓ Donnée lue : {:?}",
                String::from_utf8_lossy(&retrieved_value)
            );
        }
        None => {
            println!("✗ Clé non trouvée");
        }
    }

    db.put(b"age".to_vec(), b"25".to_vec())?;
    db.put(b"langage".to_vec(), b"Rust".to_vec())?;

    println!("\n=== Vérification des données ===");
    if let Some(age) = db.get(b"age")? {
        println!("age: {}", String::from_utf8_lossy(&age));
    }
    if let Some(lang) = db.get(b"langage")? {
        println!("langage: {}", String::from_utf8_lossy(&lang));
    }

    Ok(())
}
