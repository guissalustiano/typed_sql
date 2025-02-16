use crate::schema::*;
use std::collections::HashMap;

use tokio_postgres::{Client, Error, NoTls};

use crate::schema::Catalog;

pub(crate) async fn get<'a>(client: &'a Client) -> eyre::Result<Catalog<'a>> {
    query(&client).await
}

async fn query(client: &Client) -> eyre::Result<Catalog<'_>> {
    struct C<'a> {
        table_schema: &'a str,
        table_name: &'a str,
        column_name: &'a str,
        data_type: Type,
        is_nullable: bool,
    }

    let rows = client
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
                AND t.table_type = IN ('BASE TABLE', 'VIEW')
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
            data_type: match r.get::<_, &str>("data_type") {
                "boolean" => Type::Boolean,
                "text" | "character" | "character varying" => Type::String,
                "bytea" => Type::Bytes,
                "integer" => Type::Int,
                "real" => Type::Float,
                "bigint" => unimplemented!("data_type"),
                "date" => unimplemented!("data_type"),
                "double precision" => unimplemented!("data_type"),
                "inet" => unimplemented!("data_type"),
                "int4range" => unimplemented!("data_type"),
                "int8range" => unimplemented!("data_type"),
                "json" => unimplemented!("data_type"),
                "jsonb" => unimplemented!("data_type"),
                "numeric" => unimplemented!("data_type"),
                "smallint" => unimplemented!("data_type"),
                "timestamp with time zone" => unimplemented!("data_type"),
                "timestamp without time zone" => unimplemented!("data_type"),
                "tstzmultirange" => unimplemented!("data_type"),
                "tstzrange" => unimplemented!("data_type"),
                "uuid" => unimplemented!("data_type"),
                "ARRAY" => unimplemented!("data_type"),
                "USER-DEFINED" => unimplemented!("data_type"),
                _ => unimplemented!("data_type"),
            },
            is_nullable: r.get("is_nullable"),
        });

    todo!()
}

#[tokio::test]
async fn run() {
    let url = "postgres://postgres:bipa@localhost/bipa";
    let (client, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });
    dbg!(get(&client).await);

    panic!(".");
}
