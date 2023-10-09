/// A MemTable (memory table) holds a sorted list of MemTableEntries 
///   (records)
///
/// Writes will be duplicated to a Write-Ahead-Log for recovery in case of
///   a restart
///
/// MemTables have a max capacity which, when reached, causes the MemTable
///   to be flushed to disk as a SSTable.
///
/// Entries are stored in a Vector instead of a HashMap to allow scans
pub struct MemTable {
  entries: Vec<MemTableEntry>,
  // The size of the MemTable in units of bytes
  size: usize,
}


/// A MemTableEntry holds a key and a value.
///
/// Keys are byte sequences interpreted as strings,
///   values can be of any type.
/// 
/// A MemTable entry also contains a timestamp to record the microseconds
///   when the write occurred
/// And finally, a boolean to track tombstones for deleted items
pub struct MemTableEntry {
  pub key: Vec<u8>,
  pub value: Option<Vec<u8>>,
  pub timestamp: u128,
  pub deleted: bool,
}


impl MemTable {
  // Creates a new MemTable containing no records
  pub fn new() -> MemTable {
    MemTable {
      entries: Vec::new(),
      size: 0,
    }
  }

  pub fn set(&mut self, key: &[u8], value: &[u8], timestamp: u128) {
    let entry = MemTableEntry{
      key: key.to_owned(),
      value: Some(value.to_owned()),
      timestamp: timestamp,
      deleted: false
    };

    match self.get_index(key) {
      Ok(idx) => {
        // If the present entry at the given index contains a value, 
        //  then add differences of new and old value sizes to the MemTable
        if let Some(curr_val) = self.entries[idx].value.as_ref() {
          // If the current value is larger this will reduce size 
          //  by adding a negative value
          if curr_val.len() > value.len() {
            self.size -= curr_val.len() - value.len();
          } else {
            self.size += value.len() - curr_val.len();
          }
        }
        // Update the entry at the given location
        self.entries[idx] = entry;
      },
      Err(idx) => {
        // Increase the size of the MemTable by the size of the:
        //  key, the value, timestamp and tombstone
        // The extra size of vectors is not considered here
        self.size += key.len() + value.len() + 16 + 1;
        // Insert an entry into the vector at the given location
        self.entries.insert(idx, entry);
      }
    }
  }

  // Gets a Key-Value entry from the MemTable.
  //
  // If no record with the key exists in the MemTable, returns None
  pub fn get(&self, key: &[u8]) -> Option<&MemTableEntry> {
    if let Ok(idx) = self.get_index(key) {
      return Some(&self.entries[idx]);
    }
    None
  }

  // Performs a scan over the MemTable to find a record by value.
  //
  // If the record with the specified value is found `[Result::Ok]` is 
  //  returned, with the index of the record
  // If the record is not found then `[Result:Err]` is returned with 
  //  `usize::MAX`
  pub fn scan(&self, value: &[u8]) -> Option<&MemTableEntry> {
    for (_index, entry) in self.entries.iter().enumerate() {
      match &entry.value {
        Some(curr_val) => if value == curr_val.as_slice() {
          return Some(&entry);
        },
        None => continue
      }
    }
    None
  }

  // Deletes an entry from the MemTable.
  //
  pub fn delete(&mut self, key: &[u8], timestamp: u128) {
    let entry = MemTableEntry {
      key: key.to_owned(),
      value: None,
      timestamp: timestamp,
      deleted: true,
    };

    match self.get_index(key) {
      Ok(idx) => {
        // If the present entry at the given index contains a value, then 
        //  subtract the size of the value from the MemTable size
        if let Some(curr_val) = self.entries[idx].value.as_ref() {
          self.size -= curr_val.len();
        }
        self.entries[idx] = entry;
      },
      Err(idx) => {
        // Increase the size of the MemTable by the size of the:
        //  key, timestamp and tombstone
        self.size += key.len() + 16 + 1;
        self.entries.insert(idx, entry);
      }
    }
  }

  // Gets the number of records in the MemTable
  pub fn len(&self) -> usize {
    self.entries.len()
  }

  // Gets the total size of the records in the MemTable
  pub fn size(&self) -> usize {
    self.size
  }

  // Performs binary search over the MemTable to find a record by key
  //
  // If the record with the specified key is found `[Result::Ok]` is returned,
  //   with the index of the record
  // If the record is not found then `[Result:Err]` is returned, with the index to
  //  insert the record at.
  fn get_index(&self, key: &[u8]) -> Result<usize, usize> {
    self.entries.binary_search_by_key(&key, |entry| entry.key.as_slice())
  }
}

#[cfg(test)]
mod tests {
  use crate::mem_table::MemTable;

  #[test]
  fn test_mem_table_put_start() {
    let mut table = MemTable::new();
    table.set(b"Monday", b"Rejoice", 0);       // 13 + 16 + 1
    table.set(b"Tuesday", b"Celebrate", 10);   // 16 + 16 + 1
    // This one should go at the beginning of the table
    table.set(b"Friday",  b"Party", 21);       // 11 + 16 + 1

    assert_eq!(table.len(), 3);
    assert_eq!(table.size(), 91);

    assert_eq!(table.entries[0].key, b"Friday");
    assert_eq!(table.entries[0].value.as_ref().unwrap(), b"Party");
    assert_eq!(table.entries[0].timestamp, 21);
    assert_eq!(table.entries[0].deleted, false);


    assert_eq!(table.entries[1].key, b"Monday");
    assert_eq!(table.entries[1].value.as_ref().unwrap(), b"Rejoice");
    assert_eq!(table.entries[1].timestamp, 0);
    assert_eq!(table.entries[1].deleted, false);

    assert_eq!(table.entries[2].key, b"Tuesday");
    assert_eq!(table.entries[2].value.as_ref().unwrap(), b"Celebrate");
    assert_eq!(table.entries[2].timestamp, 10);
    assert_eq!(table.entries[2].deleted, false);
  }

  #[test]
  fn test_mem_table_put_middle() {
    let mut table = MemTable::new();

    table.set(b"Tuesday", b"Celebrate", 10);
    table.set(b"Friday", b"Party", 21);
    // This one goes into the middle of the table
    table.set(b"Monday", b"Rejoice", 0);

    assert_eq!(table.len(), 3);
    assert_eq!(table.size(), 91);

    assert_eq!(table.entries[0].key, b"Friday");
    assert_eq!(table.entries[0].value.as_ref().unwrap(), b"Party");
    assert_eq!(table.entries[0].timestamp, 21);
    assert_eq!(table.entries[0].deleted, false);


    assert_eq!(table.entries[1].key, b"Monday");
    assert_eq!(table.entries[1].value.as_ref().unwrap(), b"Rejoice");
    assert_eq!(table.entries[1].timestamp, 0);
    assert_eq!(table.entries[1].deleted, false);

    assert_eq!(table.entries[2].key, b"Tuesday");
    assert_eq!(table.entries[2].value.as_ref().unwrap(), b"Celebrate");
    assert_eq!(table.entries[2].timestamp, 10);
    assert_eq!(table.entries[2].deleted, false); 
  }

  #[test]
  fn test_mem_table_get_exists() {
    let mut table = MemTable::new();

    table.set(b"Monday", b"Rejoice", 0);
    table.set(b"Tuesday", b"Celebrate", 10);
    table.set(b"Friday", b"Party", 21);
    
    let entry = table.get(b"Monday").unwrap();
    assert_eq!(entry.key, b"Monday");
    assert_eq!(entry.value.as_ref().unwrap(), b"Rejoice");
    assert_eq!(entry.timestamp, 0);
    assert_eq!(entry.deleted, false);
  }

  #[test]
  fn test_mem_table_get_not_exists() {
    let mut table = MemTable::new();

    table.set(b"Monday", b"Rejoice", 0);
    table.set(b"Tuesday", b"Celebrate", 10);
    table.set(b"Friday", b"Party", 21);
    
    let entry = table.get(b"Thursday");
    assert_eq!(entry.is_some(), false);
  }

  #[test]
  fn test_mem_table_scan_exists() {
    let mut table = MemTable::new();

    table.set(b"Monday", b"Rejoice", 0);
    table.set(b"Tuesday", b"Celebrate", 10);
    table.set(b"Friday", b"Party", 21);

    let entry = table.scan(b"Party").unwrap();
    assert_eq!(entry.key, b"Friday");
    assert_eq!(entry.value.as_ref().unwrap(), b"Party");
    assert_eq!(entry.timestamp, 21);
    assert_eq!(entry.deleted, false);
  }

  #[test]
  fn test_mem_table_scan_not_exists() {
    let mut table = MemTable::new();
    
    table.set(b"Monday", b"Rejoice", 0);
    table.set(b"Tuesday", b"Celebrate", 10);
    table.set(b"Friday", b"Party", 21);

    let entry = table.scan(b"Blues");
    assert_eq!(entry.is_some(), false);  
  }

  #[test]
  fn test_mem_table_put_overwrite() {
    let mut table = MemTable::new();

    table.set(b"Monday", b"Rejoice", 0);
    table.set(b"Tuesday", b"Celebrate", 10);
    table.set(b"Friday", b"Party", 21);
    
    assert_eq!(table.len(), 3);
    assert_eq!(table.size(), 91);

    assert_eq!(table.entries[1].key, b"Monday");
    assert_eq!(table.entries[1].value.as_ref().unwrap(), b"Rejoice");
    assert_eq!(table.entries[1].timestamp, 0);
    assert_eq!(table.entries[1].deleted, false);

    table.set(b"Monday", b"Blues", 25);

    assert_eq!(table.len(), 3);
    assert_eq!(table.size(), 89);
    
    assert_eq!(table.entries[1].key, b"Monday");
    assert_eq!(table.entries[1].value.as_ref().unwrap(), b"Blues");
    assert_eq!(table.entries[1].timestamp, 25);
    assert_eq!(table.entries[1].deleted, false);
  }

  #[test]
  fn test_mem_table_delete_exists() {
    let mut table = MemTable::new();

    table.set(b"Monday", b"Rejoice", 0);
    table.set(b"Tuesday", b"Celebrate", 10);
    table.set(b"Friday", b"Party", 21);

    table.delete(b"Monday", 30);
    assert_eq!(table.len(), 3);
    assert_eq!(table.size(), 84);

    let entry = table.get(b"Monday").unwrap();
    assert_eq!(entry.key, b"Monday");
    assert_eq!(entry.value, None);
    assert_eq!(entry.timestamp, 30);
    assert_eq!(entry.deleted, true);
  }

  #[test]
  fn test_mem_table_delete_not_exists() {
    let mut table = MemTable::new();

    table.set(b"Monday", b"Rejoice", 0);
    table.set(b"Tuesday", b"Celebrate", 10);
    table.set(b"Friday", b"Party", 21);

    let entry = table.get(b"Thursday");
    assert_eq!(entry.is_some(), false);

    table.delete(b"Thursday", 30);
    assert_eq!(table.len(), 4);
    assert_eq!(table.size(), 116);

    let entry = table.get(b"Thursday").unwrap();
    assert_eq!(entry.key, b"Thursday");
    assert_eq!(entry.value, None);
    assert_eq!(entry.timestamp, 30);
    assert_eq!(entry.deleted, true);
  }
}
