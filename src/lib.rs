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
    fn find_type(&self, t_name: &str, c_name: &str) -> Option<ColumnData> {
        self.tables
            .iter()
            .find(|t| t.name == t_name)?
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
        NodeEnum::SelectStmt(s) => s
            .target_list
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

                        dbg!(&(t_name, c_name));
                        ctg.find_type(t_name, c_name).unwrap()
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
            .collect(),
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
        create table a(x text not null, y int not null);
        create table b(w text not null, z int not null);
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
                            data: ColumnData::int(),
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
        let expected = vec![C::string(), C::int()];

        assert_eq!(solve_type(&ctl, ast), expected);
    }

    #[test]
    fn resolve_simple_select_with_literal() {
        let ctl = tables_fixture();

        let ast = parse("SELECT y.d, 1, '123', NULL FROM y");
        let expected = vec![C::bytes(), C::int(), C::string(), C::null()];

        assert_eq!(solve_type(&ctl, ast), expected);
    }
}
