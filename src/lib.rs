#![feature(box_patterns)]

use core::panic;
use std::collections::{HashMap, HashSet};

pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

#[derive(Debug, Clone)]
struct Table {
    name: String,
    columns: HashSet<String>,
}

impl Table {
    pub(crate) fn new<'a>(name: &'a str, columns: impl IntoIterator<Item = &'a str>) -> Self {
        Table {
            name: name.to_string(),
            columns: columns.into_iter().map(ToString::to_string).collect(),
        }
    }
}

#[derive(Debug)]
struct SystemCatalogs {
    tables: HashMap<String, Table>,
}

impl SystemCatalogs {
    pub(crate) fn new() -> Self {
        SystemCatalogs {
            tables: HashMap::new(),
        }
    }

    pub(crate) fn find(ts: Vec<Table>, c: String) -> Table {
        let ts: Vec<_> = ts.into_iter().filter(|t| t.columns.contains(&c)).collect();
        if ts.len() == 0 {
            panic!("no column with this name");
        }
        if ts.len() > 1 {
            panic!("ambiguous column");
        }

        ts.first().unwrap().clone()
    }

    pub(crate) fn insert(mut self, t: Table) -> Self {
        self.tables.insert(t.name.clone(), t);
        self
    }
}

fn resolve(sc: &SystemCatalogs, ast: &sqlparser::ast::Statement) -> Result<(), ()> {
    match ast {
        sqlparser::ast::Statement::Query(box sqlparser::ast::Query {
            with: None,
            body:
                s @ box sqlparser::ast::SetExpr::Select(box sqlparser::ast::Select {
                    projection,
                    from,
                    ..
                }),
            order_by: None,
            ..
        }) => {
            let sqlparser::ast::TableFactor::Table { ref name, .. } =
                from.first().unwrap().relation
            else {
                unimplemented!()
            };
            let from = &name.0.first().unwrap().value;
            let table = sc.tables.get(from).expect("table not found");

            let sqlparser::ast::SelectItem::UnnamedExpr(sqlparser::ast::Expr::Identifier(
                sqlparser::ast::Ident { value: c, .. },
            )) = projection.first().unwrap()
            else {
                unimplemented!()
            };

            let c_table = SystemCatalogs::find(vec![table.clone()], c.to_string()).name;

            dbg!(&c_table);
            todo!()
        }
        _ => unimplemented!(),
    }
}

#[cfg(test)]
fn parse(sql: &str) -> sqlparser::ast::Statement {
    sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::PostgreSqlDialect {}, sql)
        .unwrap()
        .first()
        .unwrap()
        .clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_simple_select() {
        let table = SystemCatalogs::new().insert(Table::new("x", ["a", "b", "c"]));

        let ast = parse("SELECT a, b FROM x");
        let ir = resolve(&table, &ast).unwrap();
    }
}
