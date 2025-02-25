use crate::schema::*;
use pg_query::{
    Node, NodeEnum,
    protobuf::{JoinType, a_const::Val},
};

impl Catalog<'_> {
    fn as_ctx(&self) -> Ctx {
        self.tables
            .iter()
            .flat_map(|t| {
                t.columns
                    .iter()
                    .map(|c| CtxEntry::new(t.name, c.name, c.data))
            })
            .collect()
    }
}

pub type Ctx<'a> = Vec<CtxEntry<'a>>;

#[derive(PartialEq, Debug, Clone, Copy)]
pub struct CtxEntry<'a> {
    pub table: Option<&'a str>,
    pub column: Option<&'a str>,
    pub data: ColumnData,
}
impl<'a> CtxEntry<'a> {
    pub(crate) fn new(table: &'a str, column: &'a str, data: ColumnData) -> Self {
        Self {
            table: Some(table),
            column: Some(column),
            data,
        }
    }

    pub(crate) fn new_anonymous(data: ColumnData) -> Self {
        Self {
            table: None,
            column: None,
            data,
        }
    }
}

fn solve_from_table<'a>(sys_ctx: &Ctx<'a>, n: &NodeEnum) -> Ctx<'a> {
    let mut ctx = Vec::new();
    match n {
        NodeEnum::RangeVar(rv) => {
            ctx.extend(sys_ctx.iter().filter(|e| e.table == Some(&rv.relname)));
            ctx
        }
        NodeEnum::JoinExpr(je) => {
            let lctx = solve_from_table(
                sys_ctx,
                je.larg
                    .as_ref()
                    .expect("larg")
                    .node
                    .as_ref()
                    .expect("from.node"),
            );
            ctx.extend(lctx);

            let rctx = solve_from_table(
                sys_ctx,
                je.rarg
                    .as_ref()
                    .expect("rarg")
                    .node
                    .as_ref()
                    .expect("from.node"),
            );
            match je.jointype() {
                JoinType::JoinInner => ctx.extend(rctx),
                JoinType::JoinLeft => ctx.extend(rctx.iter().map(|e| CtxEntry {
                    data: ColumnData {
                        nullable: true,
                        ..e.data
                    },
                    ..*e
                })),
                _ => unimplemented!("join type"),
            };

            ctx
        }
        _ => unimplemented!("relname"),
    }
}

pub(crate) fn solve_from<'a>(sys_ctx: &Ctx<'a>, from: &[Node]) -> Ctx<'a> {
    from.iter()
        .map(|n| solve_from_table(sys_ctx, n.node.as_ref().expect("from.node")))
        .flatten()
        .collect()
}

pub(crate) fn solve_targets<'a>(ctx: Ctx<'a>, targets: &'a [Node]) -> Ctx<'a> {
    fn solve_target<'a>(ctx: &Ctx<'a>, n: &'a Node) -> CtxEntry<'a> {
        let NodeEnum::ResTarget(target) = n.node.as_ref().unwrap() else {
            unimplemented!("target")
        };
        let alias_name = target.name.as_str();
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
                let e = *ctx
                    .iter()
                    .find(|e| {
                        e.table.as_deref() == Some(&t_name) && e.column.as_deref() == Some(c_name)
                    })
                    .expect("selected table/name not found");

                match alias_name {
                    "" => e,
                    alias_name => CtxEntry {
                        table: None,
                        column: Some(alias_name),
                        ..e
                    },
                }
            }
            NodeEnum::AConst(c) => match c.val.as_ref() {
                Some(Val::Ival(_)) => CtxEntry::new_anonymous(ColumnData::int()),
                Some(Val::Fval(_)) => CtxEntry::new_anonymous(ColumnData::float()),
                Some(Val::Boolval(_)) => CtxEntry::new_anonymous(ColumnData::boolean()),
                Some(Val::Sval(_)) => CtxEntry::new_anonymous(ColumnData::string()),
                Some(Val::Bsval(_)) => CtxEntry::new_anonymous(ColumnData::bytes()),
                None => CtxEntry::new_anonymous(ColumnData::null()),
            },
            _ => unimplemented!("column"),
        }
    }

    targets.iter().map(|n| solve_target(&ctx, n)).collect()
}

pub fn solve_type<'a>(sys_ctx: &Ctx<'a>, stmt: &'a NodeEnum) -> Ctx<'a> {
    match stmt {
        NodeEnum::SelectStmt(s) => {
            let ctx = solve_from(sys_ctx, &s.from_clause);
            solve_targets(ctx, &s.target_list)
        }
        NodeEnum::DeleteStmt(s) => {
            if s.returning_list.is_empty() {
                return vec![CtxEntry::new_anonymous(ColumnData::int())];
            }
            let ctx = solve_from_table(
                &sys_ctx,
                &NodeEnum::RangeVar(s.relation.clone().expect("relation")),
            );
            solve_targets(ctx, &s.returning_list)
        }
        NodeEnum::InsertStmt(s) => {
            if s.returning_list.is_empty() {
                return vec![CtxEntry::new_anonymous(ColumnData::int())];
            }
            let ctx = solve_from_table(
                &sys_ctx,
                &NodeEnum::RangeVar(s.relation.clone().expect("relation")),
            );
            solve_targets(ctx, &s.returning_list)
        }
        NodeEnum::UpdateStmt(s) => {
            if s.returning_list.is_empty() {
                return vec![CtxEntry::new_anonymous(ColumnData::int())];
            }
            let ctx = solve_from_table(
                &sys_ctx,
                &NodeEnum::RangeVar(s.relation.clone().expect("relation")),
            );
            solve_targets(ctx, &s.returning_list)
        }
        _ => unimplemented!("stmt"),
    }
}

#[cfg(test)]
pub(crate) mod tests {
    type C = ColumnData;
    use crate::code_gen::parse;

    use super::*;

    #[cfg(test)]
    pub(crate) fn tables_ctx_fixture() -> Ctx<'static> {
        /*
        create table x(a text not null, b int);
        create table y(c int not null, d bytea not null);
        */

        Catalog {
            tables: vec![
                Table {
                    name: "x",
                    columns: vec![
                        Column {
                            name: "a",
                            data: ColumnData::string(),
                        },
                        Column {
                            name: "b",
                            data: ColumnData::int_nullable(),
                        },
                    ],
                },
                Table {
                    name: "y",
                    columns: vec![
                        Column {
                            name: "c",
                            data: ColumnData::int(),
                        },
                        Column {
                            name: "d",
                            data: ColumnData::bytes(),
                        },
                    ],
                },
                Table {
                    name: "w",
                    columns: vec![Column {
                        name: "e",
                        data: ColumnData::int(),
                    }],
                },
            ],
        }
        .as_ctx()
    }

    #[test]
    fn resolve_simple_select() {
        let ctl = tables_ctx_fixture();

        let ast = parse("SELECT x.a, x.b FROM x");
        let expected = vec![
            CtxEntry::new("x", "a", C::string()),
            CtxEntry::new("x", "b", C::int_nullable()),
        ];

        assert_eq!(solve_type(&ctl, &ast), expected);
    }

    #[test]
    fn resolve_simple_select_with_literal() {
        let ctl = tables_ctx_fixture();

        let ast = parse("SELECT y.d, 1, '123', NULL FROM y");
        let expected = vec![
            CtxEntry::new("y", "d", C::bytes()),
            CtxEntry::new_anonymous(C::int()),
            CtxEntry::new_anonymous(C::string()),
            CtxEntry::new_anonymous(C::null()),
        ];

        assert_eq!(solve_type(&ctl, &ast), expected);
    }

    #[test]
    #[should_panic(expected = "selected table/name not found")]
    fn resolve_based_on_from() {
        let ctl = tables_ctx_fixture();

        // x is not present on from clause
        let ast = parse("SELECT x.a FROM y");

        solve_type(&ctl, &ast);
    }

    #[test]
    fn left_join_is_marked_as_null() {
        let ctl = tables_ctx_fixture();

        let ast = parse("SELECT x.a, y.c FROM x LEFT JOIN y ON x.b = y.c");
        let expected = vec![
            CtxEntry::new("x", "a", C::string()),
            CtxEntry::new("y", "c", C::int_nullable()),
        ];

        assert_eq!(solve_type(&ctl, &ast), expected);
    }

    #[test]
    fn inner_join_is_not_marked_as_null() {
        let ctl = tables_ctx_fixture();

        let ast = parse("SELECT x.a, y.c FROM x INNER JOIN y ON x.b = y.c");
        let expected = vec![
            CtxEntry::new("x", "a", C::string()),
            CtxEntry::new("y", "c", C::int()),
        ];

        assert_eq!(solve_type(&ctl, &ast), expected);
    }

    #[test]
    fn multiple_join_works() {
        let ctl = tables_ctx_fixture();

        let ast =
            parse("SELECT x.a, y.c, w.e FROM x LEFT JOIN y ON x.b = y.c INNER JOIN w ON x.b = w.e");
        let expected = vec![
            CtxEntry::new("x", "a", C::string()),
            CtxEntry::new("y", "c", C::int_nullable()),
            CtxEntry::new("w", "e", C::int()),
        ];

        assert_eq!(solve_type(&ctl, &ast), expected);
    }

    #[test]
    fn select_support_alias() {
        let ctl = tables_ctx_fixture();

        let ast = parse("SELECT x.a as v FROM x");
        let expected = vec![CtxEntry {
            table: None,
            column: Some("v"),
            data: C::string(),
        }];

        assert_eq!(solve_type(&ctl, &ast), expected);
    }

    #[test]
    fn supports_delete() {
        let ctl = tables_ctx_fixture();

        let ast = parse("DELETE FROM x WHERE x.b < 0");
        let expected = vec![CtxEntry::new_anonymous(C::int())];

        assert_eq!(solve_type(&ctl, &ast), expected);
    }

    #[test]
    fn supports_delete_with_returning() {
        let ctl = tables_ctx_fixture();

        let ast = parse("DELETE FROM x WHERE x.b < 0 returning x.a");
        let expected = vec![CtxEntry::new("x", "a", C::string())];

        assert_eq!(solve_type(&ctl, &ast), expected);
    }

    #[test]
    fn supports_insert() {
        let ctl = tables_ctx_fixture();

        let ast = parse("INSERT INTO x(a) VALUES('a')");
        let expected = vec![CtxEntry::new_anonymous(C::int())];

        assert_eq!(solve_type(&ctl, &ast), expected);
    }

    #[test]
    fn supports_insert_with_returning() {
        let ctl = tables_ctx_fixture();

        let ast = parse("INSERT INTO x(a) VALUES('a') returning x.a");
        let expected = vec![CtxEntry::new("x", "a", C::string())];

        assert_eq!(solve_type(&ctl, &ast), expected);
    }

    #[test]
    fn supports_update() {
        let ctl = tables_ctx_fixture();

        let ast = parse("UPDATE x SET a='a1'");
        let expected = vec![CtxEntry::new_anonymous(C::int())];

        assert_eq!(solve_type(&ctl, &ast), expected);
    }

    #[test]
    fn supports_update_with_returning() {
        let ctl = tables_ctx_fixture();

        let ast = parse("UPDATE x SET a='a1' returning x.a");
        let expected = vec![CtxEntry::new("x", "a", C::string())];

        assert_eq!(solve_type(&ctl, &ast), expected);
    }
}
