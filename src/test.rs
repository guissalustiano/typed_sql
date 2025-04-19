use std::sync::{Arc, OnceLock, Weak};

use testcontainers_modules::{
    postgres::Postgres,
    testcontainers::{ContainerAsync, ImageExt},
};
use tokio::sync::Mutex;

use crate::translate_file;

pub(crate) async fn db_transaction() -> (
    Arc<ContainerAsync<Postgres>>,
    tokio_postgres::Transaction<'static>,
) {
    use testcontainers_modules::testcontainers::runners::AsyncRunner;
    // https://github.com/testcontainers/testcontainers-rs/issues/707#issuecomment-2248314261
    static C: OnceLock<Mutex<Weak<ContainerAsync<Postgres>>>> = OnceLock::new();

    let mut guard = C.get_or_init(|| Mutex::new(Weak::new())).lock().await;
    let c = if let Some(c) = guard.upgrade() {
        c
    } else {
        let c = testcontainers_modules::postgres::Postgres::default()
            .with_tag("16-alpine")
            .with_container_name("pg-sqlc-test")
            .start()
            .await
            .unwrap();
        let c = Arc::new(c);
        *guard = Arc::downgrade(&c);

        c
    };
    let host_port = c.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@localhost:{host_port}/postgres");

    let (client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });
    // TODO: client pool
    let client = Box::leak(Box::new(client));
    let t = client.transaction().await.unwrap();

    (c, t)
}

#[tokio::test]
async fn basic() {
    let (_c, t) = db_transaction().await;
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
