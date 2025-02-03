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
    #[cfg(test)]
    fn new(tables: Vec<Table>) -> Self {
        Catalog { tables }
    }

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

#[cfg(test)]
macro_rules! tables {
    (
        $($table_name:ident {
            $($column_name:ident => $column_type:ident),* $(,)?
        }),* $(,)?
    ) => {
        {
            let mut tables = Vec::new();

            $(
                let mut columns = Vec::new();

                $(
                    columns.push(Column {
                        name: stringify!($column_name).to_string(),
                        data: ColumnData{
                            type_: Type::$column_type,
                        }
                    });
                )*

                tables.push(Table {
                    name: stringify!($table_name).to_string(),
                    columns: columns,
                });
            )*

            tables
        }
    };
}

#[cfg(test)]
macro_rules! ttys {
    (
        $($type:ident),*
    ) => {
        {
            let mut c = Vec::new();

            $(
                c.push(ColumnData{
                    type_: Type::$type,
                });
            )*

            c
        }
    };
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

                        ctg.find_type(t_name, c_name).unwrap()
                    }
                    NodeEnum::AConst(c) => match c.val.as_ref() {
                        Some(Val::Ival(_)) => ColumnData { type_: Type::Int },
                        Some(Val::Fval(_)) => ColumnData { type_: Type::Float },
                        Some(Val::Boolval(_)) => ColumnData {
                            type_: Type::Boolean,
                        },
                        Some(Val::Sval(_)) => ColumnData {
                            type_: Type::String,
                        },
                        Some(Val::Bsval(_)) => ColumnData { type_: Type::Bytes },
                        None => ColumnData { type_: Type::Null },
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

    #[test]
    fn resolve_simple_select() {
        let ctl = Catalog::new(tables!(
            x {
                a => String,
                b => Int,
            },
        ));

        let ast = parse("SELECT x.a, x.b FROM x");
        let expected = ttys![String, Int];

        assert_eq!(solve_type(&ctl, ast), expected);
    }

    #[test]
    fn resolve_simple_select_with_literal() {
        let ctl = Catalog::new(tables!(
            x {
                a => Bytes,
            },
        ));

        let ast = parse("SELECT x.a, 1, '123' FROM x");
        let expected = ttys![Bytes, Int, String];

        assert_eq!(solve_type(&ctl, ast), expected);
    }

    #[test]
    fn resolve_simple_select_with_null() {
        let ctl = Catalog::new(tables!(
            x {
                a => Bytes,
            },
        ));

        let ast = parse("SELECT NULL FROM x");
        let expected = ttys![Null];

        assert_eq!(solve_type(&ctl, ast), expected);
    }
}
