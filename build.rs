use std::env;

use dotenvy::dotenv;
use sqlx::postgres::PgPoolOptions;

#[tokio::main]
async fn main() -> Result<(), sqlx::Error> {
    // trigger recompilation when a new migration is added
    println!("cargo:rerun-if-changed=src/meta/postgres/migrations");

    if Ok("release".to_string()) == env::var("PROFILE") {
        return Ok(());
    }

    dotenv().ok();
    if Ok("true".to_string()) == env::var("SQLX_OFFLINE") {
        return Ok(());
    }

    let database_url = env::var("DATABASE_URL").expect("Environment variable DATABASE_URL missing");

    let pool = PgPoolOptions::new().connect(&database_url).await?;

    sqlx::migrate!("src/meta/postgres/migrations")
        .run(&pool)
        .await?;

    Ok(())
}
