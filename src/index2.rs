#![allow(dead_code)]

use crate::error::AppError;
use crate::index2::files::FileList;
use crate::index2::id::Ids;
use crate::index2::word_map::WordMap;
use crate::index2::words::{WordData, WordList};
use blockfile::FileBlocks;
use std::io;
use std::path::Path;
use std::str::from_utf8;

type BlkNr = u32;
type BlkIdx = u32;
type FileId = u32;
type WordId = u32;
type Id = u32;

#[derive(Debug)]
pub struct Words {
    pub db: FileBlocks,
    pub ids: Ids,
    pub words: WordList,
    pub files: FileList,
    pub wordmap: WordMap,
}

#[derive(Debug)]
pub struct WordID(u32);
#[derive(Debug)]
pub struct FileID(u32);

impl Words {
    pub fn read(file: &Path) -> Result<Self, AppError> {
        let mut db = FileBlocks::open(file)?;

        let mut ids = Ids::load(&mut db)?;
        ids.create("word");
        ids.create("file");
        ids.create("wordmap");

        let words = WordList::load(&mut db)?;
        let files = FileList::load(&mut db)?;

        let wordmap_block_nr = ids.get("wordmap");
        let wordmap = WordMap::load(&mut db, wordmap_block_nr)?;

        Ok(Self {
            db,
            ids,
            words,
            files,
            wordmap,
        })
    }

    pub fn write(&mut self) -> Result<(), AppError> {
        self.words.store(&mut self.db)?;
        self.files.store(&mut self.db)?;
        let last_wordmap_block_nr = self.wordmap.store(&mut self.db)?;
        self.ids.set("wordmap", last_wordmap_block_nr);
        self.ids.store(&mut self.db)?;

        self.db.store()?;
        Ok(())
    }

    pub fn add_file(&mut self, file: String) -> u32 {
        let file_id = self.ids.next("file");
        self.files.add(file_id, file);
        file_id
    }

    pub fn remove_file(&mut self, name: String) {
        let find = self
            .files
            .list
            .iter()
            .find(|(_, file)| file.name == name)
            .map(|v| v.0)
            .cloned();
        if let Some(file_id) = find {
            self.files.list.remove(&file_id);
        }
    }

    pub fn add_word<S: AsRef<str> + Into<String>>(
        &mut self,
        word: S,
        file_idx: u32,
    ) -> Result<(), AppError> {
        if let Some(word) = self.words.list.get_mut(word.as_ref()) {
            let (new_block_nr, new_idx) = self.wordmap.add(
                &mut self.db,
                word.file_map_block_nr,
                word.file_map_block_idx,
                file_idx,
            )?;
            word.file_map_block_nr = new_block_nr;
            word.file_map_block_idx = new_idx;
        } else {
            let word_id = self.ids.next("word");
            let (word_block_nr, word_idx) = self.wordmap.add_initial(&mut self.db, file_idx);

            self.words.list.insert(
                word.as_ref().into(),
                WordData {
                    id: word_id,
                    file_map_block_nr: word_block_nr,
                    file_map_block_idx: word_idx,
                },
            );
        };
        Ok(())
    }

    //
}

pub mod word_map {
    use crate::index2::{BlkIdx, BlkNr, FileId};
    use blockfile::{BlockType, FileBlocks, BLOCK_SIZE};
    use std::fmt::{Debug, Formatter};
    use std::mem::size_of;
    use std::{io, mem};

    pub struct WordMap {
        pub last_block_nr: BlkNr,
        pub last_idx: BlkIdx,
        pub last: Box<RawWordMapList>,
    }

    pub type RawWordMapList = [RawWordMap; BLOCK_SIZE as usize / size_of::<RawWordMap>()];

    #[derive(Clone, Copy, PartialEq, Default)]
    #[repr(C)]
    pub struct RawWordMap {
        pub file_id: [FileId; 14],
        pub next_block_nr: BlkNr,
        pub next_idx: BlkIdx,
    }

    impl Debug for WordMap {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("WordMap")
                .field("last_block_nr", &self.last_block_nr)
                .field("last_idx", &self.last_idx)
                .finish()?;
            write!(f, " = ")?;
            f.debug_list().entries(self.last.iter()).finish()?;
            Ok(())
        }
    }

    impl Debug for RawWordMap {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "file_id: ")?;
            for i in 0..self.file_id.len() {
                write!(f, "{} ", self.file_id[i])?;
            }
            write!(
                f,
                "next: block_nr: {} idx: {}",
                self.next_block_nr, self.next_idx
            )?;
            Ok(())
        }
    }

    impl WordMap {
        pub const TY: BlockType = BlockType::User4;

        pub fn load(db: &mut FileBlocks, last_block_nr: BlkNr) -> Result<WordMap, io::Error> {
            if last_block_nr > 0 {
                let empty = RawWordMap::default();

                let last = db.get_owned_as::<RawWordMapList>(last_block_nr)?;

                let mut last_idx = 0u32;
                for i in 0..last.len() {
                    if last[i] == empty {
                        last_idx = i as u32;
                    }
                }
                assert_ne!(0, last_idx);

                Ok(Self {
                    last_block_nr,
                    last_idx,
                    last,
                })
            } else {
                let (last_block_nr, last) = db.alloc_owned_as::<RawWordMapList>(Self::TY);

                Ok(Self {
                    last_block_nr,
                    last_idx: 0,
                    last,
                })
            }
        }

        pub fn store(&mut self, db: &mut FileBlocks) -> Result<BlkNr, io::Error> {
            db.set_owned_as(self.last_block_nr, self.last.clone());
            Ok(self.last_block_nr)
        }

        /// Add first reference for a new word.
        pub fn add_initial(&mut self, db: &mut FileBlocks, file_id: FileId) -> (BlkNr, BlkIdx) {
            if self.last_idx + 1 >= self.last.len() as u32 {
                let (new_block_nr, new_block) = db.alloc_owned_as::<RawWordMapList>(Self::TY);

                db.set_owned_as(self.last_block_nr, mem::replace(&mut self.last, new_block));

                self.last_block_nr = new_block_nr;
                self.last_idx = 0;
            } else {
                self.last_idx += 1;
            }

            self.last[self.last_idx as usize].file_id[0] = file_id;

            (self.last_block_nr, self.last_idx)
        }

        /// Add one more file reference for a word.
        pub fn add(
            &mut self,
            db: &mut FileBlocks,
            blk_nr: BlkNr,
            blk_idx: BlkIdx,
            file_id: FileId,
        ) -> Result<(BlkNr, BlkIdx), io::Error> {
            let mut append = true;

            let mut block = db.get_owned_as::<RawWordMapList>(blk_nr)?;
            let word_map = &mut block[blk_idx as usize];
            for i in 0..word_map.file_id.len() {
                if word_map.file_id[i] == 0 {
                    word_map.file_id[i] = file_id;
                    append = false;
                    break;
                }
            }

            // no more space in this region, add a new one.
            if append {
                let (new_block_nr, new_idx) = self.add_initial(db, file_id);

                word_map.next_block_nr = new_block_nr;
                word_map.next_idx = new_idx;

                db.set_owned_as(blk_nr, block);

                Ok((new_block_nr, new_idx))
            } else {
                db.set_owned_as(blk_nr, block);

                Ok((blk_nr, blk_idx))
            }
        }
    }
}

pub mod files {
    use crate::index2::{byte_to_str, copy_clip_left, FileId};
    use blockfile::{BlockType, FileBlocks, BLOCK_SIZE};
    use std::collections::BTreeMap;
    use std::fmt::{Debug, Formatter};
    use std::io;
    use std::mem::size_of;
    use std::str::from_utf8;

    #[derive(Debug)]
    pub struct FileList {
        pub list: BTreeMap<FileId, FileData>,
    }

    #[derive(Debug)]
    pub struct FileData {
        pub name: String,
    }

    pub type RawFileList = [RawFile; BLOCK_SIZE as usize / size_of::<RawFile>()];

    #[derive(Clone, Copy, PartialEq)]
    #[repr(C)]
    pub struct RawFile {
        file: [u8; 124],
        id: FileId,
    }

    impl Debug for RawFile {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(
                f,
                "{} {}",
                self.id,
                from_utf8(&self.file).unwrap_or("?").trim_end_matches('\0')
            )
        }
    }

    impl Default for RawFile {
        fn default() -> Self {
            Self {
                file: [0u8; 124],
                id: 0,
            }
        }
    }

    impl FileList {
        pub(crate) const TY: BlockType = BlockType::User3;

        pub fn load(db: &mut FileBlocks) -> Result<FileList, io::Error> {
            let mut files = FileList {
                list: Default::default(),
            };

            let blocks: Vec<_> = db
                .iter_metadata()
                .filter(|v| v.1 == Self::TY)
                .map(|v| v.0)
                .collect();
            let empty = RawFile::default();
            for block_nr in blocks {
                let raw: Box<RawFileList> = db.get_owned_as(block_nr)?;

                for r in raw.iter() {
                    if r.file != empty.file {
                        let file = byte_to_str(&r.file)?;
                        files.list.insert(r.id, FileData { name: file.into() });
                    }
                }
            }

            Ok(files)
        }

        pub fn store(&self, db: &mut FileBlocks) -> Result<(), io::Error> {
            let mut blocks: Vec<_> = db
                .iter_metadata()
                .filter(|v| v.1 == Self::TY)
                .map(|v| v.0)
                .collect();

            let empty = RawFile::default();

            let mut it_files = self.list.iter();
            let mut it_fin = false;
            loop {
                let block = if let Some(block_nr) = blocks.pop() {
                    db.set_dirty(block_nr, true);
                    db.get_as_mut::<RawFileList>(block_nr)?
                } else {
                    if !it_fin {
                        db.alloc_as::<RawFileList>(Self::TY).1
                    } else {
                        break;
                    }
                };

                for file_rec in block.iter_mut() {
                    *file_rec = empty;
                    if let Some((file_id, file_data)) = it_files.next() {
                        copy_clip_left(file_data.name.as_bytes(), &mut file_rec.file);
                        file_rec.id = *file_id;
                    } else {
                        it_fin = true;
                    }
                }
            }

            Ok(())
        }

        pub fn add(&mut self, id: FileId, name: String) {
            self.list.insert(id, FileData { name });
        }
    }
}

pub mod words {
    use crate::index2::{byte_to_str, copy_clip, BlkIdx, BlkNr, WordId};
    use blockfile::{BlockType, FileBlocks, BLOCK_SIZE};
    use std::collections::BTreeMap;
    use std::io;
    use std::mem::size_of;

    #[derive(Debug)]
    pub struct WordList {
        pub list: BTreeMap<String, WordData>,
    }

    #[derive(Debug, Clone, Copy)]
    pub struct WordData {
        pub id: WordId,
        pub file_map_block_nr: BlkNr,
        pub file_map_block_idx: BlkIdx,
    }

    pub type RawWordList = [RawWord; BLOCK_SIZE as usize / size_of::<RawWord>()];

    #[derive(Debug, Clone, Copy, PartialEq)]
    #[repr(C)]
    pub struct RawWord {
        word: [u8; 20],
        id: WordId,
        file_map_block_nr: BlkNr,
        file_map_idx: BlkIdx,
    }

    impl Default for RawWord {
        fn default() -> Self {
            Self {
                word: Default::default(),
                id: 0,
                file_map_block_nr: 0,
                file_map_idx: 0,
            }
        }
    }

    impl WordList {
        pub const TY: BlockType = BlockType::User2;

        pub fn load(db: &mut FileBlocks) -> Result<WordList, io::Error> {
            let mut words = WordList {
                list: Default::default(),
            };

            let blocks: Vec<_> = db
                .iter_metadata()
                .filter(|v| v.1 == Self::TY)
                .map(|v| v.0)
                .collect();
            let empty = RawWord::default();
            for block_nr in blocks {
                let raw: Box<RawWordList> = db.get_owned_as(block_nr)?;

                for r in raw.iter() {
                    if r.word != empty.word {
                        let word = byte_to_str(&r.word)?;
                        words.list.insert(
                            word.into(),
                            WordData {
                                id: r.id,
                                file_map_block_nr: r.file_map_block_nr,
                                file_map_block_idx: r.file_map_idx,
                            },
                        );
                    }
                }
            }

            Ok(words)
        }

        pub fn store(&self, db: &mut FileBlocks) -> Result<(), io::Error> {
            let mut blocks: Vec<_> = db
                .iter_metadata()
                .filter(|v| v.1 == Self::TY)
                .map(|v| v.0)
                .collect();

            let empty = RawWord::default();

            let mut it_words = self.list.iter();
            loop {
                let block = if let Some(block_nr) = blocks.pop() {
                    db.set_dirty(block_nr, true);
                    db.get_as_mut::<RawWordList>(block_nr)?
                } else {
                    db.alloc_as::<RawWordList>(Self::TY).1
                };

                for word_rec in block.iter_mut() {
                    *word_rec = empty;

                    if let Some((word, word_data)) = it_words.next() {
                        copy_clip(word.as_bytes(), &mut word_rec.word);
                        word_rec.id = word_data.id;
                        word_rec.file_map_block_nr = word_data.file_map_block_nr;
                        word_rec.file_map_idx = word_data.file_map_block_idx;
                    };
                }

                if block[block.len() - 1] == empty {
                    break;
                }
            }
            Ok(())
        }
    }
}

pub mod id {
    use crate::index2::{byte_to_str, copy_clip, Id};
    use blockfile::{BlockType, FileBlocks, BLOCK_SIZE};
    use std::collections::HashMap;
    use std::io;
    use std::mem::size_of;

    #[derive(Debug)]
    pub struct Ids {
        pub ids: HashMap<String, Id>,
    }

    pub type RawIdMap = [RawId; BLOCK_SIZE as usize / size_of::<RawId>()];

    #[derive(Debug, Clone, Copy, PartialEq)]
    #[repr(C)]
    pub struct RawId {
        name: [u8; 28],
        id: Id,
    }

    impl Default for RawId {
        fn default() -> Self {
            Self {
                name: Default::default(),
                id: 0,
            }
        }
    }

    impl Ids {
        pub const TY: BlockType = BlockType::User1;

        pub fn load(db: &mut FileBlocks) -> Result<Ids, io::Error> {
            let mut ids = Ids {
                ids: Default::default(),
            };

            let blocks: Vec<_> = db
                .iter_metadata()
                .filter(|v| v.1 == Self::TY)
                .map(|v| v.0)
                .collect();

            let empty = RawId::default();
            for block_nr in blocks {
                let raw: Box<RawIdMap> = db.get_owned_as(block_nr)?;

                for r in raw.iter() {
                    if r.name != empty.name {
                        let name = byte_to_str(&r.name)?;
                        ids.ids.insert(name.into(), r.id);
                    }
                }
            }

            Ok(ids)
        }

        pub fn store(&self, db: &mut FileBlocks) -> Result<(), io::Error> {
            let mut blocks: Vec<_> = db
                .iter_metadata()
                .filter(|v| v.1 == Self::TY)
                .map(|v| v.0)
                .collect();

            let empty = RawId {
                name: Default::default(),
                id: 0,
            };

            let mut it_names = self.ids.iter();
            loop {
                let block = if let Some(block_nr) = blocks.pop() {
                    db.set_dirty(block_nr, true);
                    db.get_as_mut::<RawIdMap>(block_nr)?
                } else {
                    db.alloc_as::<RawIdMap>(Self::TY).1
                };

                for id_rec in block.iter_mut() {
                    *id_rec = empty;
                    if let Some((name, id)) = it_names.next() {
                        copy_clip(name.as_bytes(), &mut id_rec.name);
                        id_rec.id = *id;
                    }
                }

                if block[block.len() - 1] == empty {
                    break;
                }
            }

            Ok(())
        }

        pub fn create(&mut self, name: &str) {
            if !self.ids.contains_key(name) {
                self.ids.insert(name.into(), 0);
            }
        }

        pub fn next(&mut self, name: &str) -> Id {
            let last_id = self.ids.get_mut(name).expect("name");
            *last_id += 1;
            *last_id
        }

        pub fn set(&mut self, name: &str, value: Id) {
            self.ids.insert(name.into(), value);
        }

        pub fn get(&mut self, name: &str) -> Id {
            *self.ids.get(name).unwrap_or(&0u32)
        }
    }
}

fn copy_clip(src: &[u8], dst: &mut [u8]) {
    if src.len() < dst.len() {
        dst[0..src.len()].copy_from_slice(src);
    } else {
        dst.copy_from_slice(&src[0..dst.len()]);
    }
}

fn copy_clip_left(src: &[u8], dst: &mut [u8]) {
    if src.len() < dst.len() {
        dst[0..src.len()].copy_from_slice(src);
    } else {
        let start = src.len() - dst.len();
        dst.copy_from_slice(&src[start..]);
    }
}

fn byte_to_str<const N: usize>(src: &[u8; N]) -> Result<&str, io::Error> {
    let Ok(word) = from_utf8(src.as_ref()) else {
        return Err(io::Error::from(io::ErrorKind::InvalidData));
    };
    let word = word.trim_end_matches('\0');
    Ok(word)
}

#[cfg(test)]
mod tests {
    use crate::error::AppError;
    use crate::index2::files::{RawFile, RawFileList};
    use crate::index2::id::{RawId, RawIdMap};
    use crate::index2::word_map::{RawWordMap, RawWordMapList};
    use crate::index2::words::{RawWord, RawWordList};
    use crate::index2::Words;
    use blockfile::BLOCK_SIZE;
    use std::fs;
    use std::mem::size_of;
    use std::path::PathBuf;
    use std::str::FromStr;

    #[test]
    fn test_sizes() {
        println!("RawWordmapList {}", size_of::<RawWordMapList>());
        println!("RawWordMap {}", size_of::<RawWordMap>());
        println!("RawFileList {}", size_of::<RawFileList>());
        println!("RawFile {}", size_of::<RawFile>());
        println!("RawWordList {}", size_of::<RawWordList>());
        println!("RawWord {}", size_of::<RawWord>());
        println!("RawIds {}", size_of::<RawIdMap>());
        println!("RawSingleId {}", size_of::<RawId>());

        assert_eq!(BLOCK_SIZE as usize, size_of::<RawWordMapList>());
        assert_eq!(0, BLOCK_SIZE as usize % size_of::<RawWordMap>());
        assert_eq!(BLOCK_SIZE as usize, size_of::<RawFileList>());
        assert_eq!(0, BLOCK_SIZE as usize % size_of::<RawFile>());
        assert_eq!(BLOCK_SIZE as usize, size_of::<RawWordList>());
        assert_eq!(0, BLOCK_SIZE as usize % size_of::<RawWord>());
        assert_eq!(BLOCK_SIZE as usize, size_of::<RawIdMap>());
        assert_eq!(0, BLOCK_SIZE as usize % size_of::<RawId>());
    }

    #[test]
    fn test_init() -> Result<(), AppError> {
        let path = PathBuf::from_str("tmp/words.idx")?;
        let mut w = Words::read(&path)?;
        w.write()?;
        let w = Words::read(&path)?;
        dbg!(w);
        Ok(())
    }

    #[test]
    fn test_files() -> Result<(), AppError> {
        let path = PathBuf::from_str("tmp/words.idx")?;

        let _ = fs::remove_file(&path);

        let mut w = Words::read(&path)?;
        let fid = w.add_file("file0".into());
        // dbg!(&w);
        w.write()?;
        let w = Words::read(&path)?;
        dbg!(&w);
        Ok(())
    }
}
