#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Type {
    Integer,
    Text,
    Bytea,
    Boolean,
    Real,
    Unknow,
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
            type_: Type::Integer,
            nullable: false,
        }
    }
    pub fn int_nullable() -> Self {
        ColumnData {
            type_: Type::Integer,
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
            type_: Type::Real,
            nullable: false,
        }
    }
    pub fn null() -> Self {
        ColumnData {
            type_: Type::Unknow,
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
