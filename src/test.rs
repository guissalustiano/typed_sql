use crate::translate_file;

pub(crate) async fn db_transaction() -> tokio_postgres::Transaction<'static> {
    let url = "postgres://postgres:bipa@localhost/sqlc";
    let (client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });
    let client = Box::leak(Box::new(client));
    client.transaction().await.unwrap()
}

#[tokio::test]
async fn basic() {
    let t = db_transaction().await;
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
