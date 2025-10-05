use std::collections::HashSet;

#[derive(Default, Debug, Clone)]
pub struct ProjectionColumns {
    set: HashSet<String>,
}

impl ProjectionColumns {
    pub fn new() -> Self {
        Self { set: HashSet::new() }
    }

    pub fn add(&mut self, name: impl Into<String>) {
        self.set.insert(name.into());
    }

    pub fn add_many<I: IntoIterator<Item = String>>(&mut self, iter: I) {
        self.set.extend(iter);
    }

    pub fn union(mut self, other: ProjectionColumns) -> ProjectionColumns {
        self.set.extend(other.set);
        self
    }

    pub fn contains(&self, name: &str) -> bool {
        self.set.contains(name)
    }

    pub fn is_empty(&self) -> bool {
        self.set.is_empty()
    }

    pub fn len(&self) -> usize {
        self.set.len()
    }

    pub fn into_vec(self) -> Vec<String> {
        self.set.into_iter().collect()
    }
}


