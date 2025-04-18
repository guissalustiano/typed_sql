mod code_gen;
mod schema;

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let url = "postgres://postgres:bipa@localhost/sqlc";
    let (mut _client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    Ok(())
}
