use rust_database::DatabaseConfig;

fn main() {
    let data = DatabaseConfig::new();
    println!("{:?}", data);
}
