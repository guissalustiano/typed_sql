use std::ops::Deref;

use convert_case::{Case, Casing};
use eyre::ContextCompat;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};

use crate::schema::PrepareStatement;

pub(crate) fn gen_fn(data: PrepareStatement) -> eyre::Result<TokenStream> {
    fn quote_type(ty: &str) -> eyre::Result<TokenStream> {
        use crate::schema::types::*;

        Ok(match ty {
            INT4 => quote! { pub Option<i32> },
            TEXT => quote! { pub Option<String> },
            _ => eyre::bail!("type {ty} not found"),
        })
    }

    let pascal_name = data.name.to_case(Case::Pascal);
    let rows_struct_ident = format_ident!("{}Rows", pascal_name);
    let params_struct_ident = format_ident!("{}Params", pascal_name);

    let fn_name = format_ident!("{}", data.name);
    let sql_statement = data
        .statement
        .split(" AS ")
        .nth(1)
        .context("weird prepare statement")?;

    let has_params = !data.parameter_types.is_empty();
    let params_struct = if has_params {
        let param_types = data
            .parameter_types
            .iter()
            .map(Deref::deref)
            .map(quote_type)
            .collect::<eyre::Result<Vec<_>>>()?;

        quote! {
            pub struct #params_struct_ident(#(#param_types),*);
        }
    } else {
        quote! {}
    };

    let rows_struct = {
        let result_fields = data
            .result_types
            .iter()
            .map(Deref::deref)
            .map(quote_type)
            .collect::<eyre::Result<Vec<_>>>()?;
        quote! {
            pub struct #rows_struct_ident(#(#result_fields),*);
        }
    };

    // Generate param binding for the query
    let param_binding = if has_params {
        // Create parameter references for binding
        let param_refs = (0..data.parameter_types.len())
            .map(|i| {
                let i = proc_macro2::Literal::usize_unsuffixed(i);
                quote! { p.#i }
            })
            .collect::<Vec<_>>();

        quote! { &[#(#param_refs),*] }
    } else {
        quote! { &[] }
    };

    let try_get_expressions = {
        let get_exprs = (0..data.result_types.len())
            .map(|i| {
                let i = proc_macro2::Literal::usize_unsuffixed(i);
                quote! { r.try_get(#i)? }
            })
            .collect::<Vec<_>>();

        quote! { #(#get_exprs),* }
    };

    // Generate the function body with the appropriate try_get expressions
    Ok(quote! {
        #params_struct
        #rows_struct

        pub async fn #fn_name(
            c: impl tokio_postgres::GenericClient,
            p: #params_struct_ident
        ) -> Result<Vec<#rows_struct_ident>, tokio_postgres::Error> {
            c.query(#sql_statement, #param_binding).await.map(|rs| {
                rs.into_iter()
                    .map(|r| #rows_struct_ident(#try_get_expressions))
                    .collect()
            })
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::types::*;

    fn g(p: PrepareStatement) -> String {
        prettyplease::unparse(&syn::parse2(gen_fn(p).unwrap()).unwrap())
    }

    #[test]
    fn prepare_with_output() {
        let p = PrepareStatement {
            name: "list_a",
            statement: "PREPARE list_a AS SELECT a.id, a.name FROM a",
            parameter_types: vec![],
            result_types: vec![INT4, TEXT],
        };

        insta::assert_snapshot!(g(p), @r#"
        pub struct ListARows(pub Option<i32>, pub Option<String>);
        pub async fn list_a(
            c: impl tokio_postgres::GenericClient,
            p: ListAParams,
        ) -> Result<Vec<ListARows>, tokio_postgres::Error> {
            c.query("SELECT a.id, a.name FROM a", &[])
                .await
                .map(|rs| {
                    rs.into_iter().map(|r| ListARows(r.try_get(0)?, r.try_get(1)?)).collect()
                })
        }
        "#);
    }

    #[test]
    fn prepare_with_input_and_output() {
        let p = PrepareStatement {
            name: "list_a",
            statement: "PREPARE list_a AS SELECT a.id, a.name FROM a WHERE a.id = $1",
            parameter_types: vec![INT4],
            result_types: vec![INT4, TEXT],
        };

        insta::assert_snapshot!(g(p), @r#"
        pub struct ListAParams(pub Option<i32>);
        pub struct ListARows(pub Option<i32>, pub Option<String>);
        pub async fn list_a(
            c: impl tokio_postgres::GenericClient,
            p: ListAParams,
        ) -> Result<Vec<ListARows>, tokio_postgres::Error> {
            c.query("SELECT a.id, a.name FROM a WHERE a.id = $1", &[p.0])
                .await
                .map(|rs| {
                    rs.into_iter().map(|r| ListARows(r.try_get(0)?, r.try_get(1)?)).collect()
                })
        }
        "#);
    }
}
