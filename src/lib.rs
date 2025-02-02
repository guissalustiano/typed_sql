use core::panic;

use pg_query::{protobuf::ParseResult, NodeEnum};

#[derive(Debug, Clone)]
pub struct Table {
    pub(crate) name: String,
    pub(crate) columns: Vec<String>,
}

impl Table {
    #[cfg(test)]
    pub(crate) fn new<'a>(name: &'a str, columns: impl IntoIterator<Item = &'a str>) -> Self {
        Table {
            name: name.to_string(),
            columns: columns.into_iter().map(ToString::to_string).collect(),
        }
    }
}

#[derive(Debug)]
pub struct Catalog {
    pub(crate) tables: Vec<Table>,
}

impl Catalog {
    #[cfg(test)]
    pub(crate) fn new() -> Self {
        Catalog { tables: Vec::new() }
    }

    pub(crate) fn table(&self, t_name: &str) -> Option<&Table> {
        self.tables.iter().find(|t| t.name == t_name)
    }

    pub(crate) fn insert(mut self, t: Table) -> Self {
        self.tables.push(t);
        self
    }
}

fn full_qualify(sc: &Catalog, ast: ParseResult) -> Result<ParseResult, ()> {
    for stmt in &ast.stmts {
        let stmt = stmt
            .stmt
            .as_ref()
            .expect(".stmt")
            .node
            .as_ref()
            .expect(".stmt.node");

        match stmt {
            NodeEnum::SelectStmt(s) => {
                let from = &s
                    .from_clause
                    .iter()
                    .map(|n| {
                        let NodeEnum::RangeVar(r) = n.node.as_ref().expect("stmt.node.from.node")
                        else {
                            unimplemented!("from")
                        };
                        dbg!(r)
                    })
                    .map(|r| sc.table(&r.relname).expect("invalid table"))
                    .collect::<Vec<_>>();

                for target in &s.target_list {
                    let NodeEnum::ResTarget(target) =
                        target.node.as_ref().expect("stmt.node.target")
                    else {
                        unimplemented!("target")
                    };
                    let target = target
                        .val
                        .as_ref()
                        .expect("target.val")
                        .node
                        .as_ref()
                        .expect("target.val.node");

                    match target {
                        NodeEnum::ColumnRef(cr) => {
                            let target = &mut cr.fields.iter().map(|f| {
                                match f.node.as_ref().expect("column_ref.node") {
                                    NodeEnum::String(pg_query::protobuf::String { sval }) => {
                                        dbg!(sval)
                                    }
                                    _ => unimplemented!("column ref"),
                                }
                            });

                            // let target = cr.fields.

                            // match from.as_slice() {
                            //     &[] => unreachable!("fields len is zero"),
                            //     // column
                            //     &[_] => {}
                            //     // table.column
                            //     &[t_name, c_name] => {
                            //         // contains
                            //         let Some(t) = from.iter().find(|t| t.name == t_name) else {
                            //             panic!("table not found");
                            //         };

                            //         assert!(t.columns.contains(c_name))
                            //         // let table
                            //     }
                            //     // schema.table.column
                            //     &[_, _, _] => unimplemented!("fields with schema"),
                            //     // that's a lie, we can resolve to structs
                            //     _ => panic!("to many does not resolve to anything"),
                            // }

                            dbg!(cr);
                            todo!("todo")
                        }
                        _ => unimplemented!("column"),
                    }
                }
                todo!()
            }
            _ => unimplemented!("stmt"),
        }
    }

    Err(())
}

#[cfg(test)]
fn parse(sql: &str) -> ParseResult {
    pg_query::parse(sql).unwrap().protobuf
}

#[cfg(test)]
mod tests {
    use super::parse;
    use super::*;

    #[test]
    fn resolve_simple_select() {
        let ctl = Catalog::new().insert(Table::new("x", ["a", "b", "c"]));

        let ast = parse("SELECT x.a, b FROM x");
        let f_ast = parse("SELECT x.a, x.b FROM x");

        assert_eq!(full_qualify(&ctl, ast), Ok(f_ast))
    }
}
