use super::catalog::Catalog;
use super::schema::Schema;

impl Catalog {
    pub(crate) fn get_schema(&self, name: &str) -> Option<Schema> {
        self.schemas.read().unwrap().get(name).cloned()
    }
    pub(crate) fn current_schema(&self) -> Schema {
        let current_name = self.current_schema.read().unwrap().clone();
        self.get_schema(&current_name).unwrap()
    }
    pub(crate) fn create_schema(&self, name: String) -> Result<(), String> {
        let mut schemas = self.schemas.write().unwrap();
        if schemas.contains_key(&name) {
            return Err(format!("Schema {} already exists", name));
        }
        let schema = Schema::new(name.clone());
        schemas.insert(name, schema);
        Ok(())
    }
    pub(crate) fn set_current_schema(&self, name: String) -> Result<(), String> {
        let schemas = self.schemas.read().unwrap();
        if !schemas.contains_key(&name) {
            return Err(format!("Schema {} does not exist", name));
        }
        let mut current = self.current_schema.write().unwrap();
        *current = name;
        Ok(())
    }
} 