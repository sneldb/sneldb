use serde::{Deserialize, Serialize};

/// Enumerated field type.
/// - Max 256 variants
/// - Variants must be non-empty and unique
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EnumType {
    pub variants: Vec<String>,
}

impl EnumType {
    /// Basic size/uniqueness checks.
    pub fn is_valid(&self) -> bool {
        if self.variants.is_empty() || self.variants.len() > 256 {
            return false;
        }
        // ensure unique and non-empty
        let mut seen = std::collections::HashSet::with_capacity(self.variants.len());
        for v in &self.variants {
            if v.is_empty() || !seen.insert(v) {
                return false;
            }
        }
        true
    }
}

/// Internal field type used by the schema.
/// - Accepts common aliases (e.g., int)
/// - Nullable via `Optional(T)` (e.g., "string | null")
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FieldType {
    String,
    U64,
    I64,
    F64,
    Bool,
    Optional(Box<FieldType>),
    Enum(EnumType),
}

impl FieldType {
    /// Parse one primitive/alias (e.g., "int" -> I64, "number" -> F64).
    pub fn from_primitive_str(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "string" | "str" | "text" | "varchar" => Some(FieldType::String),
            "u64" | "uint64" => Some(FieldType::U64),
            "i64" | "int64" | "int" | "integer" => Some(FieldType::I64),
            "f64" | "float" | "double" | "number" => Some(FieldType::F64),
            "bool" | "boolean" => Some(FieldType::Bool),
            // common aliases in the codebase for timestamps
            "datetime" | "timestamp" => Some(FieldType::U64),
            _ => None,
        }
    }

    /// Parse `T | null` into `Optional(T)`.
    pub fn from_spec_with_nullable(s: &str) -> Option<Self> {
        if s.contains('|') {
            let parts: Vec<String> = s.split('|').map(|t| t.trim().to_string()).collect();
            let has_null = parts.iter().any(|p| p.eq_ignore_ascii_case("null"));
            let non_null = parts
                .iter()
                .find(|p| !p.eq_ignore_ascii_case("null"))
                .cloned();
            if let Some(nn) = non_null {
                if let Some(base) = FieldType::from_primitive_str(&nn) {
                    return if has_null {
                        Some(FieldType::Optional(Box::new(base)))
                    } else {
                        Some(base)
                    };
                }
            }
            None
        } else {
            FieldType::from_primitive_str(s)
        }
    }

    pub fn is_enum(&self) -> bool {
        matches!(self, FieldType::Enum(_))
    }
}
