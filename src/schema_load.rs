use crate::schema::*;
use itertools::Itertools;

use crate::schema::Catalog;

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
                c.data_type,
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
            data_type: match r.get("data_type") {
                "boolean" => Type::Boolean,
                "text" | "character" | "character varying" => Type::Text,
                "bytea" => Type::Bytea,
                "integer" => Type::Integer,
                "real" => Type::Real,
                "bigint"
                | "date"
                | "double precision"
                | "inet"
                | "int4range"
                | "int8range"
                | "json"
                | "jsonb"
                | "numeric"
                | "smallint"
                | "timestamp with time zone"
                | "timestamp without time zone"
                | "tstzmultirange"
                | "tstzrange"
                | "uuid"
                | "ARRAY"
                | "USER-DEFINED" => unimplemented!("data_type"),
                _ => panic!("data_type unknow"),
            },
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

#[tokio::test]
#[ignore]
async fn run() {
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
    t.query(
        "CREATE TABLE a(id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY, name TEXT)",
        &[],
    )
    .await
    .unwrap();

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
