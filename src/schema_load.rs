use std::collections::HashMap;

use eyre::ContextCompat;
use tokio_postgres::types::Oid;

use crate::schema::*;

use crate::schema::Catalog;

fn typname_to_enum(s: &str) -> Type {
    match s {
        "bool" => Type::Boolean,
        "text" | "_name" => Type::Text,
        "bytea" => Type::Bytea,
        "int4" => Type::Int4,
        "float4" => Type::Float4,
        _ => panic!("data_type unknow: {s}"),
    }
}

pub(crate) async fn catalog<'a, 'b>(
    client: &'a impl tokio_postgres::GenericClient,
) -> eyre::Result<Catalog<'b>> {
    let tables = client
        .query(
            "SELECT 
                t.table_schema,
                t.table_name,
                ARRAY_AGG(c.column_name::text) as column_name,
                ARRAY_AGG(c.udt_name::text) as udt_name,
                ARRAY_AGG(c.is_nullable::bool) as is_nullable
            FROM 
                information_schema.tables t
                JOIN information_schema.columns c 
                    ON t.table_schema = c.table_schema 
                    AND t.table_name = c.table_name
            WHERE 
                t.table_schema NOT IN ('pg_catalog', 'information_schema')
                AND t.table_type IN ('BASE TABLE', 'VIEW')
            GROUP BY 
                t.table_schema,
                t.table_name",
            &[],
        )
        .await?
        .iter()
        .map(|r| Table {
            name: r.get::<_, String>("table_name").leak(),
            columns: r
                .get::<_, Vec<&str>>("column_name")
                .into_iter()
                .zip(r.get::<_, Vec<&str>>("udt_name"))
                .zip(r.get::<_, Vec<bool>>("is_nullable"))
                .map(|((name, types), nullable)| Column {
                    name: name.to_owned().leak(),
                    data: ColumnData {
                        type_: typname_to_enum(types),
                        nullable,
                    },
                })
                .collect(),
        })
        .collect();

    Ok(Catalog { tables })
}

#[derive(Debug, PartialEq)]
struct PgType<'a> {
    pub oid: Oid,
    pub typname: &'a str,
}
async fn query_pg_types<'a, 'b>(
    client: &'a impl tokio_postgres::GenericClient,
) -> eyre::Result<Vec<PgType<'b>>> {
    client
        .query("SELECT oid, typname FROM pg_catalog.pg_type", &[])
        .await
        .map(|rs| {
            rs.iter()
                .map(|r| PgType {
                    oid: r.get::<_, Oid>("oid"),
                    typname: r.get::<_, String>("typname").leak(),
                })
                .collect()
        })
        .map_err(Into::into)
}

async fn pg_type_map<'a, 'b>(
    client: &'a impl tokio_postgres::GenericClient,
) -> eyre::Result<HashMap<Oid, &'b str>> {
    Ok(query_pg_types(client)
        .await?
        .into_iter()
        .map(|pt| (pt.oid, pt.typname))
        .collect())
}

#[derive(Debug, PartialEq)]
pub struct PrepareStatementInner<'a> {
    pub name: &'a str,
    pub statement: &'a str,
    pub parameter_types: Vec<Oid>,
    pub result_types: Vec<Oid>,
}
pub(crate) async fn query_prepare_statements<'a, 'b>(
    client: &'a impl tokio_postgres::GenericClient,
) -> eyre::Result<Vec<PrepareStatementInner<'b>>> {
    client
        .query(
            "SELECT 
                ps.name,
                ps.statement,
                ps.parameter_types::oid[],
                ps.result_types::oid[]
            FROM 
                pg_prepared_statements ps
            WHERE
                ps.from_sql = 't' -- avoid application generated prepare",
            &[],
        )
        .await
        .map(|rs| {
            rs.iter()
                .map(|r| PrepareStatementInner {
                    name: r.get::<_, String>("name").leak(),
                    statement: r.get::<_, String>("statement").leak(),
                    parameter_types: r.get::<_, Vec<Oid>>("parameter_types"),
                    result_types: r.get::<_, Vec<Oid>>("result_types"),
                })
                .collect()
        })
        .map_err(Into::into)
}

pub(crate) async fn prepare_statements<'a, 'b>(
    client: &'a impl tokio_postgres::GenericClient,
) -> eyre::Result<Vec<PrepareStatement<'b>>> {
    let oid2typname = pg_type_map(client).await?;
    let p_stmts = query_prepare_statements(client).await?;

    p_stmts
        .into_iter()
        .map(|p| {
            Ok(PrepareStatement {
                name: p.name,
                statement: p.statement,
                parameter_types: p
                    .parameter_types
                    .iter()
                    .map(|t| {
                        oid2typname
                            .get(t)
                            .copied()
                            .with_context(|| format!("{t} not found"))
                    })
                    .collect::<Result<_, _>>()?,
                result_types: p
                    .result_types
                    .iter()
                    .map(|t| {
                        oid2typname
                            .get(t)
                            .copied()
                            .with_context(|| format!("{t} not found"))
                    })
                    .collect::<Result<_, _>>()?,
            })
        })
        .collect()
}

#[tokio::test]
#[ignore]
async fn run_catalog() {
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
                            type_: Type::Int4,
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
    for stmt in [
        "CREATE TABLE a(id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY, name TEXT)",
        "PREPARE list_a AS SELECT a.id, a.name FROM a where id = $1",
    ] {
        t.execute(stmt, &[]).await.unwrap();
    }

    let ps = prepare_statements(&t).await.unwrap();

    assert_eq!(
        ps,
        vec![PrepareStatement {
            name: "list_a",
            statement: "PREPARE list_a AS SELECT a.id, a.name FROM a where id = $1",
            parameter_types: vec!["int4"],
            result_types: vec!["int4", "text"],
        }]
    )
}
