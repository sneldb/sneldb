use std::collections::HashSet;

#[derive(Default, Debug, Clone)]
pub struct ProjectionColumns {
    seen: HashSet<String>,
    ordered: Vec<String>,
}

impl ProjectionColumns {
    pub fn new() -> Self {
        Self {
            seen: HashSet::new(),
            ordered: Vec::new(),
        }
    }

    pub fn add(&mut self, name: impl Into<String>) {
        let name = name.into();
        if self.seen.insert(name.clone()) {
            self.ordered.push(name);
        }
    }

    pub fn add_many<I: IntoIterator<Item = String>>(&mut self, iter: I) {
        for item in iter {
            self.add(item);
        }
    }

    pub fn union(mut self, other: ProjectionColumns) -> ProjectionColumns {
        for name in other.ordered {
            self.add(name);
        }
        self
    }

    pub fn contains(&self, name: &str) -> bool {
        self.seen.contains(name)
    }

    pub fn is_empty(&self) -> bool {
        self.ordered.is_empty()
    }

    pub fn len(&self) -> usize {
        self.ordered.len()
    }

    pub fn into_vec(self) -> Vec<String> {
        self.ordered
    }
}
