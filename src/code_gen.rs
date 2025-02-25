use std::hash::Hash;

use convert_case::{Case, Casing};
use pg_query::NodeEnum;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};

use crate::schema::Catalog;
use crate::schema::ColumnData;
use crate::schema::PrepareStatement;
use crate::schema::Type;
use crate::type_solver::Ctx;
use crate::type_solver::solve_type;

#[derive(PartialEq, Debug)]
pub(crate) enum RustTypes {
    I32,
    String,
    VecU8,
    Bool,
    F32,
    Never,
}

#[derive(PartialEq, Debug)]
pub(crate) struct Param<'a> {
    name: &'a str,
    type_: RustTypes,
    nullable: bool,
}

#[derive(PartialEq, Debug)]
pub(crate) struct FnData<'a> {
    name: &'a str,
    params: Vec<Param<'a>>,
    statement: &'static str,
}

pub(crate) fn parse(sql: &str) -> NodeEnum {
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

fn has_unique_elements<T>(iter: T) -> bool
where
    T: IntoIterator,
    T::Item: Eq + Hash,
{
    let mut uniq = std::collections::HashSet::new();
    iter.into_iter().all(move |x| uniq.insert(x))
}

pub(crate) fn prepare<'a>(ctg: &Ctx<'a>, stmt: &'a PrepareStatement<'a>) -> FnData<'a> {
    let NodeEnum::PrepareStmt(n) = parse(stmt.statement) else {
        panic!("prepare");
    };
    debug_assert!(n.name == stmt.name);

    let query = n.query.as_ref().unwrap().node.as_ref().unwrap();
    let ctx = solve_type(&ctg, &query);

    if !has_unique_elements(ctx.iter().map(|c| c.column)) {
        panic!("duplicated names");
    }

    let Some(name_and_type): Option<Vec<(&str, ColumnData)>> = ctx
        .into_iter()
        .map(|c| c.column.map(|c_name| (c_name, c.data)))
        .collect()
    else {
        panic!("empty name");
    };

    let params = name_and_type
        .into_iter()
        .map(|(c_name, d)| {
            macro_rules! lazy_match {
                ($($sql_t:ident => $rust_t:ident),* $(,)?) => {
                    match d {
                        $(
                            ColumnData {
                                type_: Type::$sql_t,
                                nullable,
                            } => Param {
                                name: c_name.to_owned().leak(),
                                type_: RustTypes::$rust_t,
                                nullable,
                            },
                        )*
                    }
                };
            }

            lazy_match!(
                Integer => I32,
                Text => String,
                Bytea => VecU8,
                Boolean => Bool,
                Real => F32,
                Void => Never,
            )
        })
        .collect();

    FnData {
        name: &stmt.name,
        params,
        statement: query.deparse().unwrap().leak(),
    }
}

pub(crate) fn gen_fn_inner(data: FnData) -> TokenStream {
    // Convert name to PascalCase for struct name
    let struct_name = format!("{}Query", data.name.to_case(Case::Pascal));
    let struct_ident = format_ident!("{}", struct_name);

    // Create function name in snake_case
    let fn_name = format_ident!("{}", data.name.to_case(Case::Snake));

    // Generate struct fields based on params
    let fields = data.params.iter().map(|p| {
        let field_ident = format_ident!("{}", p.name);
        let field_type = match p.type_ {
            RustTypes::I32 => quote!(i32),
            RustTypes::String => quote!(String),
            RustTypes::VecU8 => quote! {Vec<u8>},
            RustTypes::Bool => quote! {bool},
            RustTypes::F32 => quote! {f32},
            RustTypes::Never => quote! {!},
        };

        if p.nullable {
            quote! {
                pub #field_ident: Option<#field_type>
            }
        } else {
            quote! {
                pub #field_ident: #field_type
            }
        }
    });

    // Generate struct field initialization
    let field_inits = data.params.iter().map(|Param { name, .. }| {
        let field_ident = format_ident!("{}", name);
        quote! {
            #field_ident: r.get(#name)
        }
    });
    let statement = data.statement;

    let output = quote! {
        pub struct #struct_ident {
            #( #fields, )*
        }

        pub async fn #fn_name(
            c: impl tokio_postgres::GenericClient,
        ) -> Result<Vec<#struct_ident>, tokio_postgres::Error> {
            c.query(#statement, &[]).await.map(|rs| {
                rs.into_iter()
                    .map(|r| #struct_ident {
                        #( #field_inits, )*
                    })
                    .collect()
            })
        }
    };

    output
}

pub fn gen_fn<'a>(ctg: &'a Ctx, stmt: &'a PrepareStatement<'a>) -> TokenStream {
    gen_fn_inner(prepare(ctg, stmt))
}

#[cfg(test)]
mod tests {
    use crate::type_solver::tests::tables_ctx_fixture;

    use super::*;

    #[test]
    fn prepare_basic() {
        let ctl = tables_ctx_fixture();
        let ps = PrepareStatement {
            name: "list_a",
            statement: "PREPARE list_a AS SELECT x.a, x.b FROM x",
            result_types: vec![Type::Integer, Type::Text],
        };

        let expected = FnData {
            name: "list_a",
            params: vec![
                Param {
                    name: "a",
                    type_: RustTypes::String,
                    nullable: false,
                },
                Param {
                    name: "b",
                    type_: RustTypes::I32,
                    nullable: true,
                },
            ],
            statement: "SELECT x.a, x.b FROM x",
        };

        assert_eq!(prepare(&ctl, &ps), expected);
    }

    #[test]
    fn gen_basic() {
        let d = FnData {
            name: "list_a",
            params: vec![
                Param {
                    name: "id",
                    type_: RustTypes::I32,
                    nullable: false,
                },
                Param {
                    name: "name",
                    type_: RustTypes::String,
                    nullable: true,
                },
            ],
            statement: "SELECT a.id, a.name FROM a",
        };

        let expected = quote! {
            pub struct ListAQuery {
                pub id: i32,
                pub name: Option<String>,
            }

            pub async fn list_a(
                c: impl tokio_postgres::GenericClient,
            ) -> Result<Vec<ListAQuery>, tokio_postgres::Error> {
                c.query("SELECT a.id, a.name FROM a", &[]).await.map(|rs| {
                    rs.into_iter()
                        .map(|r| ListAQuery {
                            id: r.get("id"),
                            name: r.get("name"),
                        })
                        .collect()
                })
            }
        };

        assert_eq!(gen_fn_inner(d).to_string(), expected.to_string());
    }
}
