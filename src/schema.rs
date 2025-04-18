#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Type {
    Int4,
    Text,
    Bytea,
    Boolean,
    Float4,
    Void,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnData {
    pub type_: Type,
    pub nullable: bool,
}

impl ColumnData {
    pub fn string() -> Self {
        ColumnData {
            type_: Type::Text,
            nullable: false,
        }
    }
    pub fn int() -> Self {
        ColumnData {
            type_: Type::Int4,
            nullable: false,
        }
    }
    pub fn int_nullable() -> Self {
        ColumnData {
            type_: Type::Int4,
            nullable: true,
        }
    }
    pub fn bytes() -> Self {
        ColumnData {
            type_: Type::Bytea,
            nullable: false,
        }
    }
    pub fn boolean() -> Self {
        ColumnData {
            type_: Type::Boolean,
            nullable: false,
        }
    }
    pub fn float() -> Self {
        ColumnData {
            type_: Type::Float4,
            nullable: false,
        }
    }
    pub fn null() -> Self {
        ColumnData {
            type_: Type::Void,
            nullable: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Column<'a> {
    pub name: &'a str,
    pub data: ColumnData,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Table<'a> {
    pub name: &'a str,
    pub columns: Vec<Column<'a>>,
}

#[derive(Debug, PartialEq)]
pub struct Catalog<'a> {
    pub tables: Vec<Table<'a>>,
}

#[derive(Debug, PartialEq)]
pub struct PrepareStatement<'a> {
    pub name: &'a str,
    pub statement: &'a str,
    pub parameter_types: Vec<&'a str>,
    pub result_types: Vec<&'a str>,
}
