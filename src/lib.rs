use pg_query::{protobuf::a_const::Val, NodeEnum};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Type {
    Int,
    String,
    Bytes,
    Boolean,
    Float,
    Null,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnData {
    pub type_: Type,
    pub nullable: bool,
}

impl ColumnData {
    fn string() -> Self {
        ColumnData {
            type_: Type::String,
            nullable: false,
        }
    }
    fn int() -> Self {
        ColumnData {
            type_: Type::Int,
            nullable: false,
        }
    }
    fn int_nullable() -> Self {
        ColumnData {
            type_: Type::Int,
            nullable: true,
        }
    }
    fn bytes() -> Self {
        ColumnData {
            type_: Type::Bytes,
            nullable: false,
        }
    }
    fn boolean() -> Self {
        ColumnData {
            type_: Type::Boolean,
            nullable: false,
        }
    }
    fn float() -> Self {
        ColumnData {
            type_: Type::Float,
            nullable: false,
        }
    }
    fn null() -> Self {
        ColumnData {
            type_: Type::Null,
            nullable: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub data: ColumnData,
}

#[derive(Debug, Clone)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
}

#[derive(Debug)]
pub struct Catalog {
    pub tables: Vec<Table>,
}

impl Catalog {
    fn find_table(&self, t_name: &str) -> Option<&Table> {
        self.tables.iter().find(|t| t.name == t_name)
    }

    fn find_type(&self, t_name: &str, c_name: &str) -> Option<ColumnData> {
        self.find_table(t_name)?
            .columns
            .iter()
            .find(|c| c.name == c_name)
            .map(|c| c.data)
    }
}

#[cfg(test)]
fn parse(sql: &str) -> NodeEnum {
    pg_query::parse(sql)
        .unwrap()
        .protobuf
        .stmts
        .first()
        .unwrap()
        .stmt
        .as_ref()
        .unwrap()
        .node
        .as_ref()
        .unwrap()
        .clone()
}

pub(crate) fn solve_type(ctg: &Catalog, stmt: NodeEnum) -> Vec<ColumnData> {
    match stmt {
        NodeEnum::SelectStmt(s) => {
            // dbg!(&s.from_clause);
            let from: Vec<_> = s
                .from_clause
                .iter()
                .map(|n| match dbg!(n.node.as_ref().expect("from.node")) {
                    NodeEnum::RangeVar(rv) => {
                        let t = ctg
                            .find_table(rv.relname.as_str())
                            .expect("table not found")
                            .clone();
                        vec![t]
                    }
                    NodeEnum::JoinExpr(je) => {
                        let NodeEnum::RangeVar(larg) = je
                            .larg
                            .as_ref()
                            .expect("larg")
                            .node
                            .as_ref()
                            .expect("larg.node")
                        else {
                            unimplemented!("larg");
                        };
                        let NodeEnum::RangeVar(rarg) = je
                            .rarg
                            .as_ref()
                            .expect("rarg")
                            .node
                            .as_ref()
                            .expect("rarg.node")
                        else {
                            unimplemented!("rarg");
                        };

                        let t1 = ctg
                            .find_table(larg.relname.as_str())
                            .expect("table not found")
                            .clone();
                        let mut t2 = ctg
                            .find_table(rarg.relname.as_str())
                            .expect("table not found")
                            .clone();
                        for c in t2.columns.iter_mut() {
                            c.data.nullable = true
                        }

                        vec![t1, t2]
                    }
                    _ => unimplemented!("relname"),
                })
                .flatten()
                .collect();
            dbg!(&from);

            s.target_list
                .iter()
                .map(|target| {
                    let NodeEnum::ResTarget(target) = target.node.as_ref().unwrap() else {
                        unimplemented!("target")
                    };
                    let target = target.val.as_ref().unwrap().node.as_ref().unwrap();

                    match target {
                        NodeEnum::ColumnRef(cr) => {
                            let &[t_name, c_name] = &cr
                                .fields
                                .iter()
                                .map(|f| match f.node.as_ref().unwrap() {
                                    NodeEnum::String(pg_query::protobuf::String { sval }) => sval,
                                    _ => unimplemented!("column ref"),
                                })
                                .collect::<Vec<_>>()[..]
                            else {
                                panic!("invalid name, use table.column")
                            };

                            // find type
                            from.iter()
                                .find(|t| &t.name == t_name)
                                .expect("selected table not found")
                                .columns
                                .iter()
                                .find(|c| &c.name == c_name)
                                .expect("selected column not found")
                                .data
                        }
                        NodeEnum::AConst(c) => match c.val.as_ref() {
                            Some(Val::Ival(_)) => ColumnData::int(),
                            Some(Val::Fval(_)) => ColumnData::float(),
                            Some(Val::Boolval(_)) => ColumnData::boolean(),
                            Some(Val::Sval(_)) => ColumnData::string(),
                            Some(Val::Bsval(_)) => ColumnData::bytes(),
                            None => ColumnData::null(),
                        },
                        _ => unimplemented!("column"),
                    }
                })
                .collect()
        }
        _ => unimplemented!("stmt"),
    }
}

#[cfg(test)]
mod tests {
    use super::parse;
    use super::*;

    type C = ColumnData;
    fn tables_fixture() -> Catalog {
        /*
        create table x(a text not null, b int);
        create table y(c int not null, d bytea not null);
        */

        Catalog {
            tables: vec![
                Table {
                    name: String::from("x"),
                    columns: vec![
                        Column {
                            name: String::from("a"),
                            data: ColumnData::string(),
                        },
                        Column {
                            name: String::from("b"),
                            data: ColumnData::int_nullable(),
                        },
                    ],
                },
                Table {
                    name: String::from("y"),
                    columns: vec![
                        Column {
                            name: String::from("c"),
                            data: ColumnData::int(),
                        },
                        Column {
                            name: String::from("d"),
                            data: ColumnData::bytes(),
                        },
                    ],
                },
            ],
        }
    }

    #[test]
    fn resolve_simple_select() {
        let ctl = tables_fixture();

        let ast = parse("SELECT x.a, x.b FROM x");
        let expected = vec![C::string(), C::int_nullable()];

        assert_eq!(solve_type(&ctl, ast), expected);
    }

    #[test]
    fn resolve_simple_select_with_literal() {
        let ctl = tables_fixture();

        let ast = parse("SELECT y.d, 1, '123', NULL FROM y");
        let expected = vec![C::bytes(), C::int(), C::string(), C::null()];

        assert_eq!(solve_type(&ctl, ast), expected);
    }

    #[test]
    #[should_panic(expected = "selected table not found")]
    fn resolve_based_on_from() {
        let ctl = tables_fixture();

        // x is not present on from clause
        let ast = parse("SELECT x.a FROM y");

        solve_type(&ctl, ast);
    }

    #[test]
    fn left_join_is_marked_as_null() {
        let ctl = tables_fixture();

        let ast = parse("SELECT x.a, y.c FROM x LEFT JOIN y ON x.b = y.c");
        let expected = vec![C::string(), C::int_nullable()];

        assert_eq!(solve_type(&ctl, ast), expected);
    }
}
