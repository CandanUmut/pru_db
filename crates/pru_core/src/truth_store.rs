use crate::atoms::{AtomId, EntityId, LiteralId, PredicateId};
use crate::errors::{PruError, Result};
use crate::manifest::Manifest;
use crate::resolver_store::ResolverStore;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::BufReader;
use std::path::{Path, PathBuf};

/// Minimal fact representation stored by the high-level API.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Fact {
    pub subject: EntityId,
    pub predicate: PredicateId,
    pub object: AtomId,
    pub source: Option<u64>,
    pub timestamp: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AtomTables {
    next_id: AtomId,
    entities: HashMap<EntityId, String>,
    predicates: HashMap<PredicateId, String>,
    literals: HashMap<LiteralId, String>,
}

impl Default for AtomTables {
    fn default() -> Self {
        Self {
            next_id: 1,
            entities: HashMap::new(),
            predicates: HashMap::new(),
            literals: HashMap::new(),
        }
    }
}

impl AtomTables {
    fn allocate_id(&mut self) -> AtomId {
        let id = self.next_id;
        self.next_id = self.next_id.saturating_add(1);
        id
    }

    fn find_by_value(&self, map: &HashMap<AtomId, String>, value: &str) -> Option<AtomId> {
        map.iter().find(|(_, v)| v == &&value).map(|(id, _)| *id)
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct FactLog {
    facts: Vec<Fact>,
}

/// A high-level store facade that keeps atom dictionaries and simple fact logs on disk.
///
/// The store is intentionally small and ergonomic while remaining compatible with the
/// segment/resolver-based engine underneath.
pub struct PruStore {
    dir: PathBuf,
    atoms: AtomTables,
    facts: FactLog,
    manifest: Manifest,
    resolver_store: Option<ResolverStore>,
}

impl PruStore {
    /// Open (or initialize) a store at the given directory.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let dir = path.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        let manifest = Manifest::load(&dir)?;
        let resolver_store = ResolverStore::open(&dir).ok();

        let atoms = Self::load_atoms(&dir)?;
        let facts = Self::load_facts(&dir)?;

        Ok(Self {
            dir,
            atoms,
            facts,
            manifest,
            resolver_store,
        })
    }

    /// Access the manifest currently loaded for this store.
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    /// Access the resolver store if resolver segments are present.
    pub fn resolver_store(&self) -> Option<&ResolverStore> {
        self.resolver_store.as_ref()
    }

    /// Insert or return an existing entity by name.
    pub fn intern_entity(&mut self, name: &str) -> Result<EntityId> {
        self.ensure_non_empty(name, "entity name")?;
        if let Some(id) = self.atoms.find_by_value(&self.atoms.entities, name) {
            return Ok(id);
        }
        let id = self.atoms.allocate_id();
        self.atoms.entities.insert(id, name.to_string());
        self.persist_atoms()?;
        Ok(id)
    }

    /// Insert or return an existing predicate by name.
    pub fn intern_predicate(&mut self, name: &str) -> Result<PredicateId> {
        self.ensure_non_empty(name, "predicate name")?;
        if let Some(id) = self.atoms.find_by_value(&self.atoms.predicates, name) {
            return Ok(id);
        }
        let id = self.atoms.allocate_id();
        self.atoms.predicates.insert(id, name.to_string());
        self.persist_atoms()?;
        Ok(id)
    }

    /// Insert or return an existing literal by value.
    pub fn intern_literal(&mut self, value: &str) -> Result<LiteralId> {
        self.ensure_non_empty(value, "literal value")?;
        if let Some(id) = self.atoms.find_by_value(&self.atoms.literals, value) {
            return Ok(id);
        }
        let id = self.atoms.allocate_id();
        self.atoms.literals.insert(id, value.to_string());
        self.persist_atoms()?;
        Ok(id)
    }

    /// Look up an entity name by id.
    pub fn get_entity_name(&self, id: EntityId) -> Option<String> {
        self.atoms.entities.get(&id).cloned()
    }

    /// Look up a predicate name by id.
    pub fn get_predicate_name(&self, id: PredicateId) -> Option<String> {
        self.atoms.predicates.get(&id).cloned()
    }

    /// Look up a literal value by id.
    pub fn get_literal_value(&self, id: LiteralId) -> Option<String> {
        self.atoms.literals.get(&id).cloned()
    }

    /// Append a fact to the local fact log.
    pub fn add_fact(&mut self, fact: Fact) -> Result<()> {
        self.ensure_atom_exists(fact.subject, "subject")?;
        self.ensure_predicate_exists(fact.predicate)?;
        self.ensure_object_exists(fact.object)?;

        self.facts.facts.push(fact);
        self.persist_facts()
    }

    /// Return all facts for a subject.
    pub fn facts_for_subject(&self, subj: EntityId) -> Result<Vec<Fact>> {
        Ok(self
            .facts
            .facts
            .iter()
            .filter(|f| f.subject == subj)
            .cloned()
            .collect())
    }

    /// Return all facts for a subject and predicate pair.
    pub fn facts_for_subject_predicate(
        &self,
        subj: EntityId,
        pred: PredicateId,
    ) -> Result<Vec<Fact>> {
        Ok(self
            .facts
            .facts
            .iter()
            .filter(|f| f.subject == subj && f.predicate == pred)
            .cloned()
            .collect())
    }

    fn ensure_non_empty(&self, value: &str, what: &str) -> Result<()> {
        if value.trim().is_empty() {
            return Err(PruError::InvalidInput(format!("{what} cannot be empty")));
        }
        Ok(())
    }

    fn ensure_atom_exists(&self, id: AtomId, label: &str) -> Result<()> {
        if self.atoms.entities.contains_key(&id) {
            return Ok(());
        }
        Err(PruError::AtomNotFound(format!("{label} id {id}")))
    }

    fn ensure_predicate_exists(&self, id: PredicateId) -> Result<()> {
        if self.atoms.predicates.contains_key(&id) {
            return Ok(());
        }
        Err(PruError::AtomNotFound(format!("predicate id {id}")))
    }

    fn ensure_object_exists(&self, id: AtomId) -> Result<()> {
        if self.atoms.entities.contains_key(&id) || self.atoms.literals.contains_key(&id) {
            Ok(())
        } else {
            Err(PruError::AtomNotFound(format!("object id {id}")))
        }
    }

    fn atoms_path(dir: &Path) -> PathBuf {
        dir.join("atoms.json")
    }

    fn facts_path(dir: &Path) -> PathBuf {
        dir.join("facts.json")
    }

    fn load_atoms(dir: &Path) -> Result<AtomTables> {
        let path = Self::atoms_path(dir);
        if !path.exists() {
            return Ok(AtomTables::default());
        }
        let f = File::open(path)?;
        let reader = BufReader::new(f);
        let mut atoms: AtomTables = serde_json::from_reader(reader)?;
        if atoms.next_id == 0 {
            atoms.next_id = 1;
        }
        Ok(atoms)
    }

    fn load_facts(dir: &Path) -> Result<FactLog> {
        let path = Self::facts_path(dir);
        if !path.exists() {
            return Ok(FactLog::default());
        }
        let f = File::open(path)?;
        let reader = BufReader::new(f);
        Ok(serde_json::from_reader(reader)?)
    }

    fn persist_atoms(&self) -> Result<()> {
        let path = Self::atoms_path(&self.dir);
        let tmp = path.with_extension("json.tmp");
        serde_json::to_writer_pretty(File::create(&tmp)?, &self.atoms)?;
        fs::rename(&tmp, &path)?;
        Ok(())
    }

    fn persist_facts(&self) -> Result<()> {
        let path = Self::facts_path(&self.dir);
        let tmp = path.with_extension("json.tmp");
        serde_json::to_writer_pretty(File::create(&tmp)?, &self.facts)?;
        fs::rename(&tmp, &path)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn basic_fact_roundtrip() {
        let tmp = tempdir().unwrap();
        let mut store = PruStore::open(tmp.path()).unwrap();

        let earth = store.intern_entity("Earth").unwrap();
        let moon = store.intern_entity("Moon").unwrap();
        let orbits = store.intern_predicate("orbits").unwrap();

        let fact = Fact {
            subject: moon,
            predicate: orbits,
            object: earth,
            source: None,
            timestamp: None,
        };
        store.add_fact(fact.clone()).unwrap();

        let all = store.facts_for_subject(moon).unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0], fact);

        let filtered = store.facts_for_subject_predicate(moon, orbits).unwrap();
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0], fact);
    }
}
