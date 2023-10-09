use std::fs::remove_file;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::BufWriter;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use crate::mem_table::MemTable;
use crate::utils::files_with_ext;
use crate::wal_iterator::WALEntry;
use crate::wal_iterator::WALIterator;


/// Write Ahead Log (WAL)
///
/// An append-only file which holds the operations performed on the 
///		MemTable.
///
/// The WAL is used to recover the contents of the MemTable when the server
/// is shutdown uncleanly.
pub struct WAL {
	path: PathBuf,
	file: BufWriter<File>,
}


impl WAL {
	// Loads the WAL files within a directory, returning a new WAL and 
	//	recovered MemTable.
	//
	// If multiple WAL files exist in the directory they're merged into one
	//	WAL
	pub fn from_dir(dir: &Path) -> io::Result<(WAL, MemTable)> {
		let mut wal_files = files_with_ext(dir, "wal");
		wal_files.sort();

		let mut new_mem_table = MemTable::new();
		let mut new_wal = WAL::new(dir)?;

		for wal_file in wal_files.iter() {
			if let Ok(wal) = WAL::from_path(wal_file) {
				for entry in wal.into_iter() {
					if entry.deleted {
						new_mem_table.delete(entry.key.as_slice(), entry.timestamp);
						new_wal.delete(entry.key.as_slice(), entry.timestamp)?;
					} else {
						new_mem_table.set(entry.key.as_slice(), 
															entry.value.as_ref().unwrap().as_slice(), 
															entry.timestamp);
						new_wal.set(entry.key.as_slice(), 
												entry.value.as_ref().unwrap().as_slice(),
												entry.timestamp)?;
					}
				}
			}
		}
		new_wal.flush().unwrap();
		wal_files.into_iter().for_each(|f| remove_file(f).unwrap());

		Ok((new_wal, new_mem_table))
	}

	// Creates a new WAL timestamped with the current time in the directory
	pub fn new(dir: &Path) -> io::Result<WAL> {
		let timestamp = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.unwrap()
			.as_micros();

		let path = Path::new(dir).join(timestamp.to_string() + ".wal");
		WAL::from_path(&path)
	}

	// Creates a WAL using the provided file path
	pub fn from_path(path: &Path) -> io::Result<WAL> {
		let file = OpenOptions::new().append(true).create(true).open(path)?;
		let file = BufWriter::new(file);

		Ok(WAL {
			path: path.to_owned(),
			file: file,
		})
	}

	// Records the set operation on a key-value pair to the WAL
	pub fn set(&mut self, key: &[u8], value: &[u8], timestamp: u128) -> io::Result<()> {
		self.file.write_all(&key.len().to_le_bytes())?;
		self.file.write_all(&(false as u8).to_le_bytes())?;
		self.file.write_all(&value.len().to_le_bytes())?;
		self.file.write_all(&key)?;
		self.file.write_all(&value)?;
		self.file.write_all(&timestamp.to_le_bytes())?;

		Ok(())
	}

	// Record a delete operation on a key to the WAL
	pub fn delete(&mut self, key: &[u8], timestamp: u128) -> io::Result<()> {
		self.file.write_all(&key.len().to_le_bytes())?;
		self.file.write_all(&(true as u8).to_le_bytes())?;
		self.file.write_all(&key)?;
		self.file.write_all(&timestamp.to_le_bytes())?;

		Ok(())
	}

	pub fn flush(&mut self) -> io::Result<()> {
		self.file.flush()
	}
}

impl IntoIterator for WAL {
	type IntoIter = WALIterator;
	type Item = WALEntry;

	// Transform a WAL into it's iterator form to iterate over WALEntrys 
	fn into_iter(self) -> WALIterator {
		WALIterator::new(self.path).unwrap()
	}
}


#[cfg(test)]
mod tests {
	use std::assert_eq;
	use std::fs::{create_dir, remove_dir_all, metadata};
	use std::path::PathBuf;
	use std::time::{SystemTime, UNIX_EPOCH};
	use rand::Rng;
	
	use crate::wal::WAL;
	use crate::wal_iterator::WALEntry;
	
	// Checks a given WAL entry against the data it is expected to contain
	fn check_entry(
		entry: &WALEntry,
		key: &[u8],
		value: Option<&[u8]>,
		timestamp: u128,
		deleted: bool,
	) {
		assert_eq!(entry.key.len(), key.len());
		assert_eq!(entry.key, key);
		assert_eq!(entry.timestamp, timestamp);
		assert_eq!(entry.deleted, deleted);

		if deleted {
			assert_eq!(entry.value, None)
		} else {
			assert_eq!(entry.value.as_ref().unwrap().len(), value.unwrap().len());
			assert_eq!(entry.value.as_ref().unwrap(), value.unwrap());
		}
	}

	#[test]
	fn test_write_one() {
		let mut rng = rand::thread_rng();
		let dir = PathBuf::from(format!("./{}/", rng.gen::<u32>()));
		create_dir(&dir).unwrap();

		let timestamp = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.unwrap()
			.as_micros();

		let mut wal = WAL::new(&dir).unwrap();
		wal.set(b"Monday", b"Rejoice", timestamp).unwrap();
		wal.flush().unwrap();

		if let Ok(wal) = WAL::from_path(&wal.path) {
			for entry in wal.into_iter() {
				check_entry(&entry, b"Monday", Some(b"Rejoice"), timestamp, false);
			}
		}
		remove_dir_all(&dir).unwrap();
	}

	#[test]
	fn test_write_many() {
		let mut rng = rand::thread_rng();
		let dir = PathBuf::from(format!("./{}/", rng.gen::<u32>()));
		create_dir(&dir).unwrap();

		let entries: Vec<(&[u8], Option<&[u8]>)> = vec![
			(b"Monday", Some(b"Rejoice")),
			(b"Tuesday", Some(b"Celebrate")),
			(b"Friday", Some(b"Party"))
		];

		let timestamp = SystemTime::now()
				.duration_since(UNIX_EPOCH)
				.unwrap()
				.as_micros();

		let mut wal = WAL::new(&dir).unwrap();
		for e in entries.iter() {
			wal.set(e.0, e.1.unwrap(), timestamp).unwrap();
		}
		wal.flush().unwrap();

		match WAL::from_path(&wal.path) {
			Err(_) => assert!(false),
			Ok(wal) => for (wal_entry, e) in wal.into_iter().zip(entries.iter()) {
				check_entry(&wal_entry, e.0, e.1, timestamp, false);
			}
		}

		remove_dir_all(&dir).unwrap();
	}

	#[test]
	fn test_write_delete() {
		let mut rng = rand::thread_rng();
		let dir = PathBuf::from(format!("./{}/", rng.gen::<u32>()));
		create_dir(&dir).unwrap();

		let entries: Vec<(&[u8], Option<&[u8]>)> = vec![
			(b"Monday", Some(b"Rejoice")),
			(b"Tuesday", Some(b"Celebrate")),
			(b"Friday",	Some(b"Party"))
		];

		let timestamp = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.unwrap()
			.as_micros();

		let mut wal = WAL::new(&dir).unwrap();
		// Insert
		for e in entries.iter() {
			wal.set(e.0, e.1.unwrap(), timestamp).unwrap();
		}
		wal.flush().unwrap();
		// Delete
		for e in entries.iter() {
			wal.delete(e.0, timestamp).unwrap();
		}
		wal.flush().unwrap();

		match WAL::from_path(&wal.path) {
			Err(_) => assert!(false),
			Ok(wal) => {
				let double_entries = [&entries[..], &entries[..]].concat();
				for (idx, (wal_entry, e)) in wal.into_iter().zip(double_entries).enumerate() {
					if idx < 3 {
						// First three entries are insertions
						check_entry(&wal_entry, e.0, e.1, timestamp, false);
					} else {
						// Next three entries are deletions
						check_entry(&wal_entry, e.0, None, timestamp, true);
					}
				}
			}
		}

		remove_dir_all(&dir).unwrap();
	}

	#[test]
	fn test_load_wal_empty() {
		let mut rng = rand::thread_rng();
		let dir = PathBuf::from(format!("./{}/", rng.gen::<u32>()));
		create_dir(&dir).unwrap();

		let (wal, mem_table) = WAL::from_dir(&dir).unwrap();
		assert_eq!(mem_table.len(), 0);

		let m = metadata(wal.path).unwrap();
		assert_eq!(m.len(), 0);

		remove_dir_all(&dir).unwrap();
	}

	#[test]
	fn test_load_wal_many() {
		let mut rng = rand::thread_rng();
		let dir = PathBuf::from(format!("./{}/", rng.gen::<u32>()));
		create_dir(&dir).unwrap();

		let entries: Vec<(&[u8], Option<&[u8]>)> = vec![
			(b"Monday", Some(b"Rejoice")),
			(b"Tuesday", Some(b"Celebrate")),
			(b"Friday", Some(b"Party"))
		];

		let mut wal = WAL::new(&dir).unwrap();
		for (idx, e) in entries.iter().enumerate() {
			wal.set(e.0, e.1.unwrap(), idx as u128).unwrap();
		}
		wal.flush().unwrap();

		let (wal, mem_table) = WAL::from_dir(&dir).unwrap();
		assert_eq!(mem_table.len(), 3);

		for (idx, (wal_entry, e)) in wal.into_iter().zip(entries.iter()).enumerate() {
			check_entry(&wal_entry, e.0, e.1, idx as u128, false);

			let table_e = mem_table.get(e.0).unwrap();
			assert_eq!(table_e.key, e.0);
			assert_eq!(table_e.value.as_ref().unwrap().as_slice(), e.1.unwrap());
			assert_eq!(table_e.timestamp, idx as u128);
		}

		remove_dir_all(&dir).unwrap();
	}
}