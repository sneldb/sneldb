use crate::engine::core::column::column_values::ColumnValues;
use crate::engine::core::column::format::PhysicalType;
use serde_json::{Number, Value};

#[derive(Clone, Debug)]
pub struct ColumnBlockSnapshot {
    phys: PhysicalType,
    values: ColumnValues,
}

impl ColumnBlockSnapshot {
    pub fn new(phys: PhysicalType, values: ColumnValues) -> Self {
        Self { phys, values }
    }

    pub fn empty() -> Self {
        Self {
            phys: PhysicalType::VarBytes,
            values: ColumnValues::empty(),
        }
    }

    #[inline]
    pub fn physical_type(&self) -> PhysicalType {
        self.phys
    }

    #[inline]
    pub fn values(&self) -> &ColumnValues {
        &self.values
    }

    #[inline]
    pub fn into_values(self) -> ColumnValues {
        self.values
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.values.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn to_strings(&self) -> Vec<String> {
        Self::values_to_strings(self.phys, &self.values)
    }

    pub fn into_strings(self) -> Vec<String> {
        let ColumnBlockSnapshot { phys, values } = self;
        Self::values_to_strings(phys, &values)
    }

    pub fn to_json_values(&self) -> Vec<Value> {
        Self::values_to_json(self.phys, &self.values)
    }

    pub fn into_json_values(self) -> Vec<Value> {
        let ColumnBlockSnapshot { phys, values } = self;
        Self::values_to_json(phys, &values)
    }

    fn values_to_strings(phys: PhysicalType, values: &ColumnValues) -> Vec<String> {
        let len = values.len();
        let mut out = Vec::with_capacity(len);
        match phys {
            PhysicalType::I64 => {
                for idx in 0..len {
                    match values.get_i64_at(idx) {
                        Some(v) => out.push(v.to_string()),
                        None => out.push(String::new()),
                    }
                }
            }
            PhysicalType::U64 => {
                for idx in 0..len {
                    match values.get_u64_at(idx) {
                        Some(v) => out.push(v.to_string()),
                        None => out.push(String::new()),
                    }
                }
            }
            PhysicalType::F64 => {
                for idx in 0..len {
                    match values.get_f64_at(idx) {
                        Some(v) => out.push(v.to_string()),
                        None => out.push(String::new()),
                    }
                }
            }
            PhysicalType::Bool => {
                for idx in 0..len {
                    match values.get_bool_at(idx) {
                        Some(true) => out.push("true".to_string()),
                        Some(false) => out.push("false".to_string()),
                        None => out.push(String::new()),
                    }
                }
            }
            _ => {
                for idx in 0..len {
                    out.push(values.get_str_at(idx).unwrap_or("").to_string());
                }
            }
        }
        out
    }

    fn values_to_json(phys: PhysicalType, values: &ColumnValues) -> Vec<Value> {
        let len = values.len();
        let mut out = Vec::with_capacity(len);
        match phys {
            PhysicalType::I64 => {
                for idx in 0..len {
                    match values.get_i64_at(idx) {
                        Some(v) => out.push(Value::Number(Number::from(v))),
                        None => out.push(Value::Null),
                    }
                }
            }
            PhysicalType::U64 => {
                for idx in 0..len {
                    match values.get_u64_at(idx) {
                        Some(v) => out.push(Value::Number(Number::from(v))),
                        None => out.push(Value::Null),
                    }
                }
            }
            PhysicalType::F64 => {
                for idx in 0..len {
                    match values.get_f64_at(idx) {
                        Some(v) => match Number::from_f64(v) {
                            Some(num) => out.push(Value::Number(num)),
                            None => out.push(Value::Null),
                        },
                        None => out.push(Value::Null),
                    }
                }
            }
            PhysicalType::Bool => {
                for idx in 0..len {
                    match values.get_bool_at(idx) {
                        Some(b) => out.push(Value::Bool(b)),
                        None => out.push(Value::Null),
                    }
                }
            }
            _ => {
                for idx in 0..len {
                    out.push(
                        values
                            .get_str_at(idx)
                            .map(|s| Value::String(s.to_string()))
                            .unwrap_or(Value::Null),
                    );
                }
            }
        }
        out
    }
}
