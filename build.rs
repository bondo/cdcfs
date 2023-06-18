use std::env;

use dotenvy::dotenv;
use sqlx::{migrate, postgres::PgPoolOptions};

#[tokio::main]
async fn main() -> Result<(), sqlx::Error> {
    // trigger recompilation when a new migration is added
    println!("cargo:rerun-if-changed=src/meta/postgres/migrations");

    dotenv().ok();
    let database_url = env::var("DATABASE_URL").expect("Environment variable DATABASE_URL missing");

    let pool = PgPoolOptions::new().connect(database_url.as_str()).await?;
    migrate!("src/meta/postgres/migrations").run(&pool).await?;

    Ok(())
}
