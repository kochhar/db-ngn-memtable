use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::BufReader;
use std::io::Read;
use std::path::PathBuf;


/// WAL Entry mirrors the MemTable entry in the mem_table module
pub struct WALEntry {
	pub key: Vec<u8>,
	pub value: Option<Vec<u8>>,
	pub timestamp: u128,
	pub deleted: bool,
}


// WAL Iterator allows iterating over the entries in a WAL file
//
// Each entry in the WAL will be stored back-to-back with enough metadata
// to recover the keys and values of the records.
pub struct WALIterator {
	reader: BufReader<File>,
}


impl WALIterator {
	pub fn new(path: PathBuf) -> io::Result<WALIterator> {
		let file = OpenOptions::new().read(true).open(path)?;
		let reader = BufReader::new(file);
		Ok(WALIterator { reader })
	}

	fn read_key(&mut self, key_len: usize) -> Option<Vec<u8>> {
		let mut key = vec![0; key_len];
		if self.reader.read_exact(&mut key).is_err() {
			return None;
		}
		Some(key)
	}

	fn read_value(&mut self, value_len: usize) -> Option<Vec<u8>> {
		let mut value = vec![0; value_len];
		if self.reader.read_exact(&mut value).is_err() {
			return None;
		}
		Some(value)
	}

	fn read_timestamp(&mut self) -> Option<u128> {
		let mut timestamp = [0; 16];
		if self.reader.read_exact(&mut timestamp).is_err() {
			return None
		}
		Some(u128::from_le_bytes(timestamp))
	}
}

impl Iterator for WALIterator {
	type Item = WALEntry;

	// +---------------+---------------+-----------------+-...-+--...--+-----------------+
	// | Key Size (8B) | Tombstone(1B) | Value Size (8B) | Key | Value | Timestamp (16B) |
	// +---------------+---------------+-----------------+-...-+--...--+-----------------+
	//
	// Key Size = Length of the Key data
	// Tombstone = If this record was deleted and has a value
	// Value Size = Length of the Value data
	// Key = Key data
	// Value = Value data
	// Timestamp = Timestamp of the operation in microseconds

	fn next(&mut self) -> Option<WALEntry> {
		let mut len_buffer = [0; 8];
		
		// First attempt to read the size of the key -- 8 bytes
		if self.reader.read_exact(&mut len_buffer).is_err() {
			return None;
		}
		let key_len = usize::from_le_bytes(len_buffer);

		// Next attempt to read if the entry is deleted of not -- 1 byte
		let mut bool_buffer = [0; 1];
		if self.reader.read_exact(&mut bool_buffer).is_err() {
			return None;
		}
		let deleted = bool_buffer[0] != 0;

		let mut key = None;
		let mut value = None;
		if deleted {
			// If it's a deleted entry, immediately read the key since there's no
			//	value len to read.
			key = self.read_key(key_len);
			if !key.is_some() {
				return None;
			}
		} else {
			// If it's not a deleted entry, read length of the value -- 8 bytes
			//	then read the key and value
			if self.reader.read_exact(&mut len_buffer).is_err() {
        return None;
      }
			let value_len = usize::from_le_bytes(len_buffer);
			
			key = self.read_key(key_len);
			value = self.read_value(value_len);
			if !key.is_some() || !value.is_some() {
				return None;
			}
		}

		// Finally read the timestamp
		let timestamp = self.read_timestamp();
		if !timestamp.is_some() {
			return None
		}

		Some(WALEntry{
			key: key.unwrap(),
			value: value,
			timestamp: timestamp.unwrap(),
			deleted: deleted,
		})
	}
}