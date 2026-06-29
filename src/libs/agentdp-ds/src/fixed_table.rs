#[derive(Debug)]
pub struct FixedTable<K, V> {
    entries: Vec<Option<(K, V)>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FixedTableReserveError {
    KeyExists,
    Full,
}

#[must_use]
pub struct FixedTableReservation<'a, K, V> {
    table: &'a mut FixedTable<K, V>,
    index: usize,
    key: K,
}

impl<K, V> FixedTable<K, V>
where
    K: Eq,
{
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        let mut entries = Vec::with_capacity(capacity);
        entries.resize_with(capacity, || None);
        Self { entries }
    }

    /// # Errors
    ///
    /// Returns the provided value when the table is full and the key is not already present.
    pub fn insert(&mut self, key: K, value: V) -> Result<Option<V>, V> {
        if let Some((_existing_key, existing_value)) = self
            .entries
            .iter_mut()
            .filter_map(Option::as_mut)
            .find(|(existing_key, _value)| *existing_key == key)
        {
            return Ok(Some(std::mem::replace(existing_value, value)));
        }

        let Some(entry) = self.entries.iter_mut().find(|entry| entry.is_none()) else {
            return Err(value);
        };
        *entry = Some((key, value));
        Ok(None)
    }

    /// # Errors
    ///
    /// Returns `KeyExists` when the key is already present, or `Full` when no vacant slot exists.
    pub fn reserve_vacant(&mut self, key: K) -> Result<FixedTableReservation<'_, K, V>, FixedTableReserveError> {
        if self
            .entries
            .iter()
            .filter_map(Option::as_ref)
            .any(|(existing_key, _value)| *existing_key == key)
        {
            return Err(FixedTableReserveError::KeyExists);
        }
        let Some(index) = self.entries.iter().position(Option::is_none) else {
            return Err(FixedTableReserveError::Full);
        };
        Ok(FixedTableReservation {
            table: self,
            index,
            key,
        })
    }

    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        self.entries
            .iter_mut()
            .filter_map(Option::as_mut)
            .find(|(existing_key, _value)| existing_key == key)
            .map(|(_key, value)| value)
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        self.entries
            .iter()
            .filter_map(Option::as_ref)
            .find(|(existing_key, _value)| existing_key == key)
            .map(|(_key, value)| value)
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        let entry = self
            .entries
            .iter_mut()
            .find(|entry| entry.as_ref().is_some_and(|(existing_key, _value)| existing_key == key))?;
        entry.take().map(|(_key, value)| value)
    }

    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.entries
            .iter()
            .filter_map(|entry| entry.as_ref().map(|(_key, value)| value))
    }

    pub fn values_mut(&mut self) -> impl Iterator<Item = &mut V> {
        self.entries
            .iter_mut()
            .filter_map(|entry| entry.as_mut().map(|(_key, value)| value))
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.iter().filter(|entry| entry.is_some()).count()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.iter().all(Option::is_none)
    }

    #[must_use]
    pub const fn capacity(&self) -> usize {
        self.entries.len()
    }
}

impl<K, V> FixedTableReservation<'_, K, V> {
    pub fn insert(self, value: V) {
        debug_assert!(self.table.entries[self.index].is_none());
        self.table.entries[self.index] = Some((self.key, value));
    }
}

impl<K, V> FixedTable<K, V>
where
    K: Copy + Eq,
{
    pub fn iter(&self) -> impl Iterator<Item = (K, &V)> {
        self.entries
            .iter()
            .filter_map(|entry| entry.as_ref().map(|(key, value)| (*key, value)))
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = (K, &mut V)> {
        self.entries
            .iter_mut()
            .filter_map(|entry| entry.as_mut().map(|(key, value)| (*key, value)))
    }

    pub fn keys_into(&self, output: &mut Vec<K>) {
        output.clear();
        output.extend(self.iter().map(|(key, _value)| key));
    }
}
