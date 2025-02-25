use convert_case::{Case, Casing};
use proc_macro2::TokenStream;
use quote::{format_ident, quote};

enum RustTypes {
    I32,
    String,
}

struct FnData<'a> {
    name: &'a str,
    params: Vec<(&'a str, RustTypes)>,
    statement: &'a str,
}

pub fn gen_fn(data: FnData) -> TokenStream {
    // Convert name to PascalCase for struct name
    let struct_name = format!("{}Query", data.name.to_case(Case::Pascal));
    let struct_ident = format_ident!("{}", struct_name);

    // Create function name in snake_case
    let fn_name = format_ident!("{}", data.name.to_case(Case::Snake));

    // Generate struct fields based on params
    let fields = data.params.iter().map(|(name, rtype)| {
        let field_ident = format_ident!("{}", name);
        let field_type = match rtype {
            RustTypes::I32 => quote!(i32),
            RustTypes::String => quote!(String),
        };
        quote! {
            pub #field_ident: #field_type
        }
    });

    // Generate struct field initialization
    let field_inits = data.params.iter().map(|(name, _)| {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn run_prepare_statement() {
        let d = FnData {
            name: "list_a",
            params: vec![("id", RustTypes::I32), ("name", RustTypes::String)],
            statement: "SELECT a.id, a.name FROM a",
        };

        let expected = quote! {
            pub struct ListAQuery {
                pub id: i32,
                pub name: String,
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

        assert_eq!(gen_fn(d).to_string(), expected.to_string());
    }
}
