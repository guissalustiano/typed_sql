use core::panic;
use std::time::Duration;

use crate::schema::*;
use itertools::Itertools;

use crate::schema::Catalog;

fn typname_to_enum(s: &str) -> Type {
    match s {
        "bool" => Type::Boolean,
        "text" | "_name" => Type::Text,
        "bytea" => Type::Bytea,
        "int4" => Type::Integer,
        "float4" => Type::Real,
        _ => panic!("data_type unknow: {s}"),
    }
}

pub(crate) async fn catalog<'a, 'b>(
    client: &'a impl tokio_postgres::GenericClient,
) -> eyre::Result<Catalog<'b>> {
    struct C {
        table_schema: String,
        table_name: String,
        column_name: String,
        data_type: Type,
        is_nullable: bool,
    }

    let tables = client
        .query(
            "SELECT 
                t.table_schema,
                t.table_name,
                c.column_name,
                c.udt_name,
                c.is_nullable::boolean
            FROM 
                information_schema.tables t
                JOIN information_schema.columns c 
                    ON t.table_schema = c.table_schema 
                    AND t.table_name = c.table_name
            WHERE 
                t.table_schema NOT IN ('pg_catalog', 'information_schema')
                AND t.table_type IN ('BASE TABLE', 'VIEW')
            ORDER BY 
                t.table_schema,
                t.table_name,
                c.ordinal_position",
            &[],
        )
        .await?
        .iter()
        .map(|r| C {
            table_schema: r.get("table_schema"),
            table_name: r.get("table_name"),
            column_name: r.get("column_name"),
            data_type: typname_to_enum(r.get("udt_name")),
            is_nullable: r.get("is_nullable"),
        })
        .chunk_by(|ref r| (r.table_schema.clone(), r.table_name.clone()))
        .into_iter()
        .map(|((_, table), r)| Table {
            name: table.leak(),
            columns: r
                .into_iter()
                .map(|c| Column {
                    name: c.column_name.leak(),
                    data: ColumnData {
                        type_: c.data_type,
                        nullable: c.is_nullable,
                    },
                })
                .collect(),
        })
        .collect();

    Ok(Catalog { tables })
}
pub(crate) async fn prepare_statements<'a, 'b>(
    client: &'a impl tokio_postgres::GenericClient,
) -> eyre::Result<PrepareStatements<'b>> {
    client
        .query(
            "SELECT 
                ps.name,
                ps.statement,
                 ARRAY_AGG(pt.typname) as type_name
            FROM 
                pg_prepared_statements ps,
                LATERAL UNNEST(ps.result_types) as rt(oid)
                LEFT JOIN pg_type pt ON pt.oid = rt.oid::oid
            WHERE
                ps.from_sql = 't' -- avoid application generated prepare
            GROUP BY 
                ps.name, ps.statement",
            &[],
        )
        .await
        .map(|rs| {
            rs.iter()
                .map(|r| PrepareStatement {
                    name: r.get::<_, String>("name").leak(),
                    statement: r.get::<_, String>("statement").leak(),
                    result_types: r
                        .get::<_, Vec<&str>>("type_name")
                        .into_iter()
                        .map(typname_to_enum)
                        .collect(),
                })
                .collect()
        })
        .map_err(Into::into)
}

#[tokio::test]
async fn run_catalog() {
    let url = "postgres://postgres:bipa@localhost/typer";
    let (mut client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let t = client.transaction().await.unwrap();
    for stmt in ["CREATE TABLE a(id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY, name TEXT)"] {
        t.execute(stmt, &[]).await.unwrap();
    }

    let catalog = catalog(&t).await.unwrap();
    assert_eq!(
        catalog,
        Catalog {
            tables: vec![Table {
                name: "a",
                columns: vec![
                    Column {
                        name: "id",
                        data: ColumnData {
                            type_: Type::Integer,
                            nullable: false,
                        },
                    },
                    Column {
                        name: "name",
                        data: ColumnData {
                            type_: Type::Text,
                            nullable: true,
                        },
                    },
                ],
            },],
        }
    );
}

#[tokio::test]
async fn run_prepare_statement() {
    let url = "postgres://postgres:bipa@localhost/typer";
    let (mut client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let t = client.transaction().await.unwrap();
    for stmt in [
        "CREATE TABLE a(id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY, name TEXT)",
        "PREPARE list_a AS SELECT a.id, a.name FROM a",
    ] {
        t.execute(stmt, &[]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let ps = prepare_statements(&t).await.unwrap();

    assert_eq!(
        ps,
        vec![PrepareStatement {
            name: "list_a",
            statement: "PREPARE list_a AS SELECT a.id, a.name FROM a",
            result_types: vec![Type::Integer, Type::Text],
        }]
    )
}
