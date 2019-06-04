use crate::{Batch, Error, KeyValueStore};
use sled::Db;
use std::sync::Arc;

pub struct Store {
    db: Arc<Db>,
}

impl<'a> KeyValueStore<'a> for Store {
    type Batch = Store;

    fn new(path: &str) -> Self {
        let db = Db::start_default(path).expect("Failed to open sled");
        let db = Arc::new(db);

        Store { db }
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let val = self.db.get(key)?.map(|v| v.to_vec());

        Ok(val)
    }

    fn exists(&self, key: &[u8]) -> Result<bool, Error> {
        self.db.contains_key(key).map_err(Into::into)
    }

    fn batch(&self) -> Result<Self::Batch, Error> {
        Ok(Self::Batch {
            db: Arc::clone(&self.db),
        })
    }
}

impl Batch for Store {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        self.db.set(key, value)?;
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<(), Error> {
        self.db.del(key)?;
        Ok(())
    }

    fn commit(self) -> Result<(), Error> {
        self.db.flush()?;
        Ok(())
    }
}

impl From<sled::Error> for Error {
    fn from(e: sled::Error) -> Error {
        Error::DBError(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile;

    #[test]
    fn put_and_get() {
        let tmp_dir = tempfile::Builder::new()
            .prefix("put_and_get")
            .tempdir()
            .unwrap();

        let store = Store::new(tmp_dir.path().to_str().unwrap());
        let mut batch = store.batch().unwrap();
        batch.put(&[0, 0], &[0, 0, 0]).unwrap();
        batch.put(&[1, 1], &[1, 1, 1]).unwrap();
        batch.commit().unwrap();

        assert_eq!(Some(vec![0, 0, 0]), store.get(&[0, 0]).unwrap());
        assert_eq!(Some(vec![1, 1, 1]), store.get(&[1, 1]).unwrap());
        assert_eq!(None, store.get(&[2, 2]).unwrap())
    }

    #[test]
    fn exists() {
        let tmp_dir = tempfile::Builder::new().prefix("exists").tempdir().unwrap();
        let store = Store::new(tmp_dir.path().to_str().unwrap());
        assert!(!store.exists(&[0, 0]).unwrap());

        let mut batch = store.batch().unwrap();
        batch.put(&[0, 0], &[0, 0, 0]).unwrap();
        batch.commit().unwrap();

        assert!(store.exists(&[0, 0]).unwrap());
    }

    #[test]
    fn delete() {
        let tmp_dir = tempfile::Builder::new().prefix("delete").tempdir().unwrap();
        let store = Store::new(tmp_dir.path().to_str().unwrap());
        let mut batch = store.batch().unwrap();
        batch.put(&[0, 0], &[0, 0, 0]).unwrap();
        batch.commit().unwrap();
        assert_eq!(Some(vec![0, 0, 0]), store.get(&[0, 0]).unwrap());

        let mut batch = store.batch().unwrap();
        batch.delete(&[0, 0]).unwrap();
        batch.commit().unwrap();
        assert_eq!(None, store.get(&[0, 0]).unwrap());
    }
}
