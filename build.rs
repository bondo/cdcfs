use std::env;

use dotenvy::dotenv;
use sqlx::postgres::PgPoolOptions;

#[tokio::main]
async fn main() -> Result<(), sqlx::Error> {
    // trigger recompilation when a new migration is added
    println!("cargo:rerun-if-changed=src/meta/postgres/migrations");

    dotenv().ok();
    if let Ok(database_url) = env::var("DATABASE_URL") {
        let pool = PgPoolOptions::new().connect(&database_url).await?;

        sqlx::migrate!("src/meta/postgres/migrations")
            .run(&pool)
            .await?;
    }

    Ok(())
}
