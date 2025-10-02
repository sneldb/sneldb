use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::sync::Mutex;

/// Simple process-global string interner for UIDs and fields.
/// Not persisted; IDs are stable only within a process lifetime.
struct StringInterner {
    map: HashMap<String, u32>,
    rev: Vec<String>,
}

impl StringInterner {
    fn new() -> Self {
        Self {
            map: HashMap::new(),
            rev: Vec::new(),
        }
    }

    fn intern(&mut self, s: &str) -> u32 {
        if let Some(&id) = self.map.get(s) {
            return id;
        }
        let id = self.rev.len() as u32;
        self.rev.push(s.to_owned());
        self.map.insert(s.to_owned(), id);
        id
    }
}

static UID_INTERNER: Lazy<Mutex<StringInterner>> = Lazy::new(|| Mutex::new(StringInterner::new()));
static FIELD_INTERNER: Lazy<Mutex<StringInterner>> =
    Lazy::new(|| Mutex::new(StringInterner::new()));

#[inline]
pub fn intern_uid(uid: &str) -> u32 {
    let mut guard = UID_INTERNER.lock().unwrap();
    guard.intern(uid)
}

#[inline]
pub fn intern_field(field: &str) -> u32 {
    let mut guard = FIELD_INTERNER.lock().unwrap();
    guard.intern(field)
}
