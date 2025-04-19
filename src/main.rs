use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt},
};

mod code_gen;
mod schema;

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
    let stmt_raw = {
        let mut b = String::new();
        sql.read_to_string(&mut b).await?;
        b
    };
    client.batch_execute(&stmt_raw).await?;

    let code = code_gen::gen_file(client).await?;
    rs.write(code.as_bytes()).await?;

    Ok(())
}
#[cfg(test)]
mod test {
    use crate::translate_file;

    #[tokio::test]
    async fn basic() {
        let url = "postgres://postgres:bipa@localhost/sqlc";
        let (mut client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .unwrap();

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        let t = client.transaction().await.unwrap();
        t.execute(
            "CREATE TABLE a(id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY, name TEXT)",
            &[],
        )
        .await
        .unwrap();

        let mut sql =
            std::io::Cursor::new("PREPARE list_a AS SELECT a.id, a.name FROM a where id = $1");
        let mut rs = std::io::Cursor::new(Vec::new());

        translate_file(&t, &mut sql, &mut rs).await.unwrap();
        let rs = String::from_utf8(rs.into_inner()).unwrap();

        insta::assert_snapshot!(rs, @r#"
        pub struct ListAParams(pub Option<i32>);
        pub struct ListARows(pub Option<i32>, pub Option<String>);
        pub async fn list_a(
            c: impl tokio_postgres::GenericClient,
            p: ListAParams,
        ) -> Result<Vec<ListARows>, tokio_postgres::Error> {
            c.query("SELECT a.id, a.name FROM a where id = $1", &[p.0])
                .await
                .map(|rs| {
                    rs.into_iter().map(|r| ListARows(r.try_get(0)?, r.try_get(1)?)).collect()
                })
        }
        "#);
    }
}
