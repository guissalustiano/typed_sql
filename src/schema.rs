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
    pub fn string() -> Self {
        ColumnData {
            type_: Type::String,
            nullable: false,
        }
    }
    pub fn int() -> Self {
        ColumnData {
            type_: Type::Int,
            nullable: false,
        }
    }
    pub fn int_nullable() -> Self {
        ColumnData {
            type_: Type::Int,
            nullable: true,
        }
    }
    pub fn bytes() -> Self {
        ColumnData {
            type_: Type::Bytes,
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
            type_: Type::Float,
            nullable: false,
        }
    }
    pub fn null() -> Self {
        ColumnData {
            type_: Type::Null,
            nullable: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Column<'a> {
    pub name: &'a str,
    pub data: ColumnData,
}

#[derive(Debug, Clone)]
pub struct Table<'a> {
    pub name: &'a str,
    pub columns: &'a [Column<'a>],
}

#[derive(Debug)]
pub struct Catalog<'a> {
    pub tables: &'a [Table<'a>],
}
