use crate::engine::schema::errors::SchemaError;
use crate::engine::schema::schema_store::SchemaStore;
use crate::engine::schema::types::{EnumType, FieldType};
use crate::shared::config::CONFIG;
use rand::{Rng, distributions::Alphanumeric};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MiniSchema {
    pub fields: HashMap<String, FieldType>,
}

impl MiniSchema {
    pub fn field_names(&self) -> HashSet<String> {
        self.fields.keys().cloned().collect()
    }

    pub fn fields(&self) -> impl Iterator<Item = &String> {
        self.fields.keys()
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SchemaRecord {
    pub uid: String,
    pub event_type: String,
    pub schema: MiniSchema,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SchemaRegistry {
    schemas: HashMap<String, MiniSchema>,
    uid_map: HashMap<String, String>,
    reverse_uid_map: HashMap<String, String>,
    store: SchemaStore,
}

impl SchemaRegistry {
    pub fn new() -> Result<Self, SchemaError> {
        let def_dir = &CONFIG.schema.def_dir;
        std::fs::create_dir_all(def_dir)
            .map_err(|e| SchemaError::IoWriteFailed(format!("Creating dir: {}", e)))?;
        let path = Path::new(def_dir).join("schemas.bin");
        Self::new_with_path(path)
    }

    pub fn new_with_path(path: PathBuf) -> Result<Self, SchemaError> {
        let store = SchemaStore::new(path)?;
        let mut registry = Self {
            schemas: HashMap::new(),
            uid_map: HashMap::new(),
            reverse_uid_map: HashMap::new(),
            store,
        };
        registry.load_all()?;
        Ok(registry)
    }

    pub fn define(&mut self, event_type: &str, schema: MiniSchema) -> Result<(), SchemaError> {
        if self.schemas.contains_key(event_type) {
            return Err(SchemaError::AlreadyDefined(event_type.to_string()));
        }
        if schema.fields.is_empty() {
            return Err(SchemaError::EmptySchema);
        }

        let uid: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(16)
            .map(char::from)
            .collect();

        let record = SchemaRecord {
            uid: uid.clone(),
            event_type: event_type.to_string(),
            schema: schema.clone(),
        };

        self.store.append(&record)?;
        self.register_record(record);
        Ok(())
    }

    pub fn get(&self, event_type: &str) -> Option<&MiniSchema> {
        self.schemas.get(event_type)
    }

    pub fn get_all(&self) -> &HashMap<String, MiniSchema> {
        &self.schemas
    }

    pub fn get_uid(&self, event_type: &str) -> Option<String> {
        self.uid_map.get(event_type).cloned()
    }

    pub fn get_event_type_by_uid(&self, uid: &str) -> Option<String> {
        self.reverse_uid_map.get(uid).cloned()
    }

    pub fn get_schema_by_uid(&self, uid: &str) -> Option<&MiniSchema> {
        self.reverse_uid_map
            .get(uid)
            .and_then(|event_type| self.schemas.get(event_type))
    }

    fn load_all(&mut self) -> Result<(), SchemaError> {
        for record in self.store.load()? {
            self.register_record(record);
        }
        Ok(())
    }

    fn register_record(&mut self, record: SchemaRecord) {
        self.schemas
            .insert(record.event_type.clone(), record.schema);
        self.uid_map
            .insert(record.event_type.clone(), record.uid.clone());
        self.reverse_uid_map.insert(record.uid, record.event_type);
    }
}

impl From<crate::command::types::MiniSchema> for MiniSchema {
    fn from(cmd_schema: crate::command::types::MiniSchema) -> Self {
        let mut fields: HashMap<String, FieldType> = HashMap::new();
        for (name, spec) in cmd_schema.fields.into_iter() {
            match spec {
                crate::command::types::FieldSpec::Primitive(s) => {
                    if let Some(ft) = FieldType::from_spec_with_nullable(&s) {
                        fields.insert(name, ft);
                    } else {
                        // default to String for unknown type names for backward compatibility
                        fields.insert(name, FieldType::String);
                    }
                }
                crate::command::types::FieldSpec::Enum(variants) => {
                    let et = EnumType { variants };
                    fields.insert(name, FieldType::Enum(et));
                }
            }
        }
        Self { fields }
    }
}
