use eyre::ContextCompat;
use std::collections::HashMap;
use tokio_postgres::types::Oid;

pub(crate) mod types {
    pub const INT4: &str = "int4";
    pub const TEXT: &str = "text";
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
struct PrepareStatementInner<'a> {
    pub name: &'a str,
    pub statement: &'a str,
    pub parameter_types: Vec<Oid>,
    pub result_types: Vec<Oid>,
}
async fn query_prepare_statements<'a, 'b>(
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

#[derive(Debug, PartialEq)]
pub(crate) struct PrepareStatement<'a> {
    pub name: &'a str,
    pub statement: &'a str,
    pub parameter_types: Vec<&'a str>,
    pub result_types: Vec<&'a str>,
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

#[cfg(test)]
mod test {
    use crate::schema::{PrepareStatement, prepare_statements, types::*};
    use crate::test::db_transaction;

    #[tokio::test]
    async fn run_prepare_statement() {
        let (_c, t) = db_transaction().await;
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
                parameter_types: vec![INT4],
                result_types: vec![INT4, TEXT],
            }]
        )
    }
}
