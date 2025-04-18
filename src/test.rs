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

async fn e2e(ts: &str, ps: &str) -> String {
    let (_c, t) = db_transaction().await;
    t.execute(ts, &[]).await.unwrap();

    let mut sql = std::io::Cursor::new(ps);
    let mut rs = std::io::Cursor::new(Vec::new());

    translate_file(&t, &mut sql, &mut rs).await.unwrap();
    String::from_utf8(rs.into_inner()).unwrap()
}

#[tokio::test]
async fn without_input() {
    let rs = e2e(
        "CREATE TABLE users(id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY, name TEXT);",
        "PREPARE list_users AS SELECT u.id, u.name FROM users u;",
    )
    .await;

    insta::assert_snapshot!(rs, @r#"
    pub struct ListUsersRows {
        pub id: Option<i32>,
        pub name: Option<String>,
    }
    pub async fn list_users(
        c: impl tokio_postgres::GenericClient,
        p: ListUsersParams,
    ) -> Result<Vec<ListUsersRows>, tokio_postgres::Error> {
        c.query("SELECT u.id, u.name FROM users AS u", &[])
            .await
            .map(|rs| {
                rs.into_iter()
                    .map(|r| ListUsersRows {
                        id: r.try_get(0)?,
                        name: r.try_get(1)?,
                    })
                    .collect()
            })
    }
    "#);
}

#[tokio::test]
async fn with_input() {
    let rs = e2e(
        "CREATE TABLE users(id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY, name TEXT);",
        "PREPARE find_user AS SELECT u.id, u.name FROM users u where u.id = $1;",
    )
    .await;

    insta::assert_snapshot!(rs, @r#"
    pub struct FindUserParams {
        pub param_0: Option<i32>,
    }
    pub struct FindUserRows {
        pub id: Option<i32>,
        pub name: Option<String>,
    }
    pub async fn find_user(
        c: impl tokio_postgres::GenericClient,
        p: FindUserParams,
    ) -> Result<Vec<FindUserRows>, tokio_postgres::Error> {
        c.query("SELECT u.id, u.name FROM users AS u WHERE u.id = $1", &[p.param_0])
            .await
            .map(|rs| {
                rs.into_iter()
                    .map(|r| FindUserRows {
                        id: r.try_get(0)?,
                        name: r.try_get(1)?,
                    })
                    .collect()
            })
    }
    "#);
}

#[tokio::test]
async fn multiple_prepare() {
    let rs = e2e(
        "CREATE TABLE users(id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY, name TEXT);",
        "PREPARE list_users AS SELECT u.id, u.name FROM users u;
         PREPARE find_user AS SELECT u.id, u.name FROM users u where u.id = $1;",
    )
    .await;

    insta::assert_snapshot!(rs, @r#"
    pub struct ListUsersRows {
        pub id: Option<i32>,
        pub name: Option<String>,
    }
    pub async fn list_users(
        c: impl tokio_postgres::GenericClient,
        p: ListUsersParams,
    ) -> Result<Vec<ListUsersRows>, tokio_postgres::Error> {
        c.query("SELECT u.id, u.name FROM users AS u", &[])
            .await
            .map(|rs| {
                rs.into_iter()
                    .map(|r| ListUsersRows {
                        id: r.try_get(0)?,
                        name: r.try_get(1)?,
                    })
                    .collect()
            })
    }

    pub struct FindUserParams {
        pub param_0: Option<i32>,
    }
    pub struct FindUserRows {
        pub id: Option<i32>,
        pub name: Option<String>,
    }
    pub async fn find_user(
        c: impl tokio_postgres::GenericClient,
        p: FindUserParams,
    ) -> Result<Vec<FindUserRows>, tokio_postgres::Error> {
        c.query("SELECT u.id, u.name FROM users AS u WHERE u.id = $1", &[p.param_0])
            .await
            .map(|rs| {
                rs.into_iter()
                    .map(|r| FindUserRows {
                        id: r.try_get(0)?,
                        name: r.try_get(1)?,
                    })
                    .collect()
            })
    }
    "#);
}
