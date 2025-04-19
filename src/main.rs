use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt},
};

mod code_gen;
mod schema;
#[cfg(test)]
mod test;

#[derive(clap::Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    path: std::path::PathBuf,
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let cli: Args = clap::Parser::parse();
    let url = "postgres://postgres:bipa@localhost/sqlc";
    let (client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let mut sql = File::open(&cli.path).await?;
    let mut rs = File::open(cli.path.with_extension("rs")).await?;
    translate_file(&client, &mut sql, &mut rs).await
}

async fn translate_file(
    client: &impl tokio_postgres::GenericClient,
    sql: &mut (impl AsyncReadExt + Unpin),
    rs: &mut (impl AsyncWriteExt + Unpin),
) -> eyre::Result<()> {
    let mut stmts_raw = String::new();
    sql.read_to_string(&mut stmts_raw).await?;

    let code = code_gen::gen_file(client, stmts_raw).await?;
    rs.write(code.as_bytes()).await?;

    Ok(())
}
