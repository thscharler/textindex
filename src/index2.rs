#![allow(dead_code)]

use crate::error::AppError;
use crate::index2::files::FileList;
use crate::index2::id::Ids;
use crate::index2::word_map::{IterFileId, WordMap};
use crate::index2::words::{WordData, WordList};
use blockfile::{BlockType, FileBlocks, UserBlockType};
use std::fmt::{Debug, Formatter};
use std::io;
use std::path::Path;
use std::str::from_utf8;

type BlkNr = u32;
type BlkIdx = u32;
type FIdx = u32;
type FileId = u32;
type WordId = u32;
type Id = u32;

#[derive(Debug)]
pub struct Words {
    pub db: WordFileBlocks,
    pub ids: Ids,
    pub words: WordList,
    pub files: FileList,
    pub wordmap: WordMap,
}

pub type WordFileBlocks = FileBlocks<WordBlockType>;

#[derive(Clone, Copy, PartialEq)]
pub enum WordBlockType {
    NotAllocated = BlockType::NotAllocated as isize,
    Free = BlockType::Free as isize,
    BlockMap = BlockType::BlockMap as isize,

    Ids = BlockType::User1 as isize,
    WordList = BlockType::User2 as isize,
    FileList = BlockType::User3 as isize,
    WordMap = BlockType::User4 as isize,

    OtherUser = 254,
    Undefined = BlockType::Undefined as isize,
}

impl Debug for WordBlockType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let v = match self {
            WordBlockType::NotAllocated => "___",
            WordBlockType::Free => "FRE",
            WordBlockType::BlockMap => "BMP",
            WordBlockType::Ids => "IDS",
            WordBlockType::WordList => "WRD",
            WordBlockType::FileList => "FIL",
            WordBlockType::WordMap => "W-F",
            WordBlockType::OtherUser => "OTH",
            WordBlockType::Undefined => "UND",
        };
        write!(f, "{}", v)
    }
}

impl UserBlockType for WordBlockType {
    fn fbt(self) -> BlockType {
        match self {
            WordBlockType::NotAllocated => BlockType::NotAllocated,
            WordBlockType::Free => BlockType::Free,
            WordBlockType::BlockMap => BlockType::BlockMap,
            WordBlockType::Ids => BlockType::User1,
            WordBlockType::WordList => BlockType::User2,
            WordBlockType::FileList => BlockType::User3,
            WordBlockType::WordMap => BlockType::User4,
            WordBlockType::OtherUser => unreachable!(),
            WordBlockType::Undefined => BlockType::Undefined,
        }
    }

    fn ubt(block_type: BlockType) -> Self {
        match block_type {
            BlockType::NotAllocated => Self::NotAllocated,
            BlockType::Free => Self::Free,
            BlockType::BlockMap => Self::BlockMap,
            BlockType::User1 => Self::Ids,
            BlockType::User2 => Self::WordList,
            BlockType::User3 => Self::FileList,
            BlockType::User4 => Self::WordMap,
            BlockType::Undefined => Self::Undefined,
            _ => Self::OtherUser,
        }
    }
}

impl Words {
    pub fn read(file: &Path) -> Result<Self, AppError> {
        let mut db = FileBlocks::open(file)?;

        let mut ids = Ids::load(&mut db)?;
        ids.create("wordmap");
        ids.create("word");
        ids.create("word_block_nr");
        ids.create("word_block_idx");
        ids.create("file");
        ids.create("file_block_nr");
        ids.create("file_block_idx");

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
        let mut word_nr = self.ids.get("word_block_nr");
        let mut word_idx = self.ids.get("word_block_idx");
        self.words
            .store(&mut self.db, &mut word_nr, &mut word_idx)?;
        self.ids.set("word_block_nr", word_nr);
        self.ids.set("word_block_idx", word_idx);

        let mut file_nr = self.ids.get("file_block_nr");
        let mut file_idx = self.ids.get("file_block_idx");
        self.files
            .store(&mut self.db, &mut file_nr, &mut file_idx)?;
        self.ids.set("file_block_nr", file_nr);
        self.ids.set("file_block_idx", file_idx);

        let last_wordmap_block_nr = self.wordmap.store(&mut self.db)?;
        self.ids.set("wordmap", last_wordmap_block_nr);

        self.ids.store(&mut self.db)?;
        dbg!(&self.db);
        self.db.store()?;
        Ok(())
    }

    /// Adds a new file.
    /// It's not checked, if the same file was already added.
    /// Simply returns a new FileId.
    pub fn add_file(&mut self, file: String) -> FileId {
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

    pub fn iter_word_files(&mut self, word_data: WordData) -> IterFileId {
        WordMap::iter_files(
            &mut self.db,
            word_data.file_map_block_nr,
            word_data.file_map_block_idx,
        )
    }

    /// Add a word and a file reference.
    /// It is not checked, if the reference was already inserted.
    /// Duplicates are acceptable.
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
            let (word_block_nr, word_idx) = self.wordmap.add_initial(&mut self.db, file_idx)?;

            self.words.list.insert(
                word.as_ref().into(),
                WordData {
                    id: word_id,
                    block_nr: 0,
                    block_idx: 0,
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
    use crate::index2::{BlkIdx, BlkNr, FIdx, FileId, WordBlockType, WordFileBlocks};
    use blockfile::{Length, BLOCK_SIZE};
    use std::fmt::{Debug, Formatter};
    use std::io;
    use std::mem::size_of;

    pub struct WordMap {
        pub last_block_nr: BlkNr,
        pub last_idx: BlkIdx,
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
        pub const TY: WordBlockType = WordBlockType::WordMap;

        pub fn load(db: &mut WordFileBlocks, last_block_nr: BlkNr) -> Result<WordMap, io::Error> {
            if last_block_nr > 0 {
                let empty = RawWordMap::default();

                let last = db.get(last_block_nr)?.cast::<RawWordMapList>();
                let mut last_idx = 0u32;
                for i in 0..last.len() {
                    if last[i] == empty {
                        break;
                    }
                    last_idx = i as u32;
                }

                Ok(Self {
                    last_block_nr,
                    last_idx,
                })
            } else {
                // don't init here, do this with the first add_word.
                // uses last_block_nr == 0 as sentinel.
                Ok(Self {
                    last_block_nr: 0,
                    last_idx: 0,
                })
            }
        }

        pub fn store(&mut self, _db: &mut WordFileBlocks) -> Result<BlkNr, io::Error> {
            Ok(self.last_block_nr)
        }

        // Ensures we can add at least 1 new region.
        fn ensure_add(&mut self, db: &mut WordFileBlocks) -> BlkIdx {
            let res_idx = if self.last_block_nr == 0 {
                let new_block_nr = db.alloc(Self::TY).block_nr();

                self.last_block_nr = new_block_nr;
                self.last_idx = 0;

                self.last_idx
            } else {
                if self.last_idx + 1 >= RawWordMapList::LEN as u32 {
                    let new_block_nr = db.alloc(Self::TY).block_nr();

                    db.discard(self.last_block_nr);

                    self.last_block_nr = new_block_nr;
                    self.last_idx = 0;

                    self.last_idx
                } else {
                    self.last_idx + 1
                }
            };

            res_idx
        }

        /// Add first reference for a new word.
        pub fn add_initial(
            &mut self,
            db: &mut WordFileBlocks,
            file_id: FileId,
        ) -> Result<(BlkNr, BlkIdx), io::Error> {
            let new_idx = self.ensure_add(db);

            let block = db.get_mut(self.last_block_nr)?;
            block.set_dirty(true);

            let last = block.cast_mut::<RawWordMapList>();
            last[new_idx as usize].file_id[0] = file_id;

            self.last_idx = new_idx;

            Ok((self.last_block_nr, self.last_idx))
        }

        /// Add one more file reference for a word.
        pub fn add(
            &mut self,
            db: &mut WordFileBlocks,
            blk_nr: BlkNr,
            blk_idx: BlkIdx,
            file_id: FileId,
        ) -> Result<(BlkNr, BlkIdx), io::Error> {
            // append to given region list.
            let mut append = true;
            {
                let block = db.get_mut(blk_nr)?;
                block.set_dirty(true);

                let word_map_list = block.cast_mut::<RawWordMapList>();
                let word_map = &mut word_map_list[blk_idx as usize];
                for fid in word_map.file_id.iter_mut() {
                    if *fid == 0 {
                        *fid = file_id;
                        append = false;
                        break;
                    }
                }
            }

            // no more space in this region, add a new one.
            if append {
                // add to new region
                let new_idx = self.ensure_add(db);

                let block = db.get_mut(self.last_block_nr)?;
                block.set_dirty(true);

                let last = block.cast_mut::<RawWordMapList>();
                last[new_idx as usize].file_id[0] = file_id;
                last[new_idx as usize].next_block_nr = blk_nr;
                last[new_idx as usize].next_idx = blk_idx;

                self.last_idx = new_idx;

                Ok((self.last_block_nr, self.last_idx))
            } else {
                Ok((blk_nr, blk_idx))
            }
        }

        pub fn iter_files(
            db: &mut WordFileBlocks,
            block_nr: BlkNr,
            block_idx: BlkIdx,
        ) -> IterFileId {
            IterFileId {
                db,
                map_block_nr: block_nr,
                map_idx: block_idx,
                file_idx: 0,
            }
        }
    }

    pub struct IterFileId<'a> {
        db: &'a mut WordFileBlocks,
        map_block_nr: u32,
        map_idx: BlkIdx,
        file_idx: FIdx,
    }

    impl<'a> IterFileId<'a> {
        fn is_clear(&self) -> bool {
            self.map_block_nr == 0
        }

        fn clear(&mut self) {
            self.map_block_nr = 0;
            self.map_idx = 0;
            self.file_idx = 0;
        }
    }

    impl<'a> Iterator for IterFileId<'a> {
        type Item = Result<FileId, io::Error>;

        fn next(&mut self) -> Option<Self::Item> {
            if self.is_clear() {
                return None;
            }

            let mut discard_block = None;
            let file_id = loop {
                let map_list = match self.db.get(self.map_block_nr) {
                    Ok(block) => block.cast::<RawWordMapList>(),
                    Err(err) => return Some(Err(err)),
                };
                let map = &map_list[self.map_idx as usize];
                let file_id = map.file_id[self.file_idx as usize];

                if file_id != 0 {
                    // next
                    self.file_idx += 1;
                    if self.file_idx >= map.file_id.len() as u32 {
                        discard_block = Some(self.map_block_nr);
                        self.map_block_nr = map.next_block_nr;
                        self.map_idx = map.next_idx;
                        self.file_idx = 0;
                    }
                    break Some(file_id);
                } else {
                    if map.next_block_nr != 0 {
                        discard_block = Some(self.map_block_nr);
                        self.map_block_nr = map.next_block_nr;
                        self.map_idx = map.next_idx;
                        self.file_idx = 0;
                    } else {
                        break None;
                    }
                }
            };

            if let Some(block_nr) = discard_block {
                self.db.discard(block_nr);
            }

            file_id.map(|v| Ok(v))
        }
    }
}

pub mod files {
    use crate::index2::{
        byte_to_str, copy_fix_left, BlkIdx, BlkNr, FileId, WordBlockType, WordFileBlocks,
    };
    use blockfile::Length;
    use blockfile::BLOCK_SIZE;
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
        pub block_nr: BlkNr,
        pub block_idx: BlkIdx,
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
        pub(crate) const TY: WordBlockType = WordBlockType::FileList;

        pub fn load(db: &mut WordFileBlocks) -> Result<FileList, io::Error> {
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
                let raw = db.get(block_nr)?.cast::<RawFileList>();
                for (i, r) in raw.iter().enumerate() {
                    if r.file != empty.file {
                        let file = byte_to_str(&r.file)?;
                        files.list.insert(
                            r.id,
                            FileData {
                                name: file.into(),
                                block_nr,
                                block_idx: i as u32,
                            },
                        );
                    }
                }
                db.discard(block_nr);
            }

            Ok(files)
        }

        pub fn store(
            &mut self,
            db: &mut WordFileBlocks,
            last_block_nr: &mut u32,
            last_block_idx: &mut u32,
        ) -> Result<(), io::Error> {
            // assume append only
            for (file_id, file_data) in self.list.iter_mut() {
                if file_data.block_nr == 0 {
                    if *last_block_nr == 0 {
                        *last_block_nr = db.alloc(Self::TY).block_nr();
                        *last_block_idx = 0;
                    }

                    let w = RawFile {
                        file: copy_fix_left::<124>(file_data.name.as_bytes()),
                        id: *file_id,
                    };

                    let block = db.get_mut(*last_block_nr)?;
                    block.set_dirty(true);
                    let file_list = block.cast_mut::<RawFileList>();
                    file_list[*last_block_idx as usize] = w;
                    file_data.block_nr = *last_block_nr;
                    file_data.block_idx = *last_block_idx;

                    if *last_block_idx + 1 == RawFileList::LEN as u32 {
                        *last_block_nr = db.alloc(Self::TY).block_nr();
                        *last_block_idx = 0;
                    } else {
                        *last_block_idx += 1;
                    }
                }
            }

            Ok(())
        }

        pub fn add(&mut self, id: FileId, name: String) {
            self.list.insert(
                id,
                FileData {
                    name,
                    block_nr: 0,
                    block_idx: 0,
                },
            );
        }
    }
}

pub mod words {
    use crate::index2::{
        byte_to_str, copy_fix, BlkIdx, BlkNr, WordBlockType, WordFileBlocks, WordId,
    };
    use blockfile::Length;
    use blockfile::BLOCK_SIZE;
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
        pub block_nr: BlkNr,
        pub block_idx: BlkIdx,
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
        pub const TY: WordBlockType = WordBlockType::WordList;

        pub fn load(db: &mut WordFileBlocks) -> Result<WordList, io::Error> {
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
                let raw = db.get(block_nr)?.cast::<RawWordList>();
                for (i, r) in raw.iter().enumerate() {
                    if r.word != empty.word {
                        let word = byte_to_str(&r.word)?;
                        words.list.insert(
                            word.into(),
                            WordData {
                                id: r.id,
                                block_nr,
                                block_idx: i as u32,
                                file_map_block_nr: r.file_map_block_nr,
                                file_map_block_idx: r.file_map_idx,
                            },
                        );
                    }
                }
                db.discard(block_nr);
            }

            Ok(words)
        }

        pub fn store(
            &mut self,
            db: &mut WordFileBlocks,
            last_block_nr: &mut u32,
            last_block_idx: &mut u32,
        ) -> Result<(), io::Error> {
            // assume append only
            for (word, word_data) in self.list.iter_mut() {
                let w = RawWord {
                    word: copy_fix::<20>(word.as_bytes()),
                    id: word_data.id,
                    file_map_block_nr: word_data.file_map_block_nr,
                    file_map_idx: word_data.file_map_block_idx,
                };

                if word_data.block_nr != 0 {
                    let block = db.get_mut(word_data.block_nr)?;
                    let word_list = block.cast_mut::<RawWordList>();

                    if word_list[word_data.block_idx as usize] != w {
                        word_list[word_data.block_idx as usize] = w;
                        block.set_dirty(true);
                    }
                } else {
                    if *last_block_nr == 0 {
                        *last_block_nr = db.alloc(Self::TY).block_nr();
                        *last_block_idx = 0;
                    }

                    let block = db.get_mut(*last_block_nr)?;
                    block.set_dirty(true);
                    let word_list = block.cast_mut::<RawWordList>();
                    word_list[*last_block_idx as usize] = w;
                    word_data.block_nr = *last_block_nr;
                    word_data.block_idx = *last_block_idx;

                    if *last_block_idx + 1 == RawWordList::LEN as u32 {
                        *last_block_nr = db.alloc(Self::TY).block_nr();
                        *last_block_idx = 0;
                    } else {
                        *last_block_idx += 1;
                    }
                }
            }

            Ok(())
        }
    }
}

pub mod id {
    use crate::index2::{byte_to_str, copy_clip, Id, WordBlockType, WordFileBlocks};
    use blockfile::BLOCK_SIZE;
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
        pub const TY: WordBlockType = WordBlockType::Ids;

        pub fn load(db: &mut WordFileBlocks) -> Result<Ids, io::Error> {
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
                let raw = db.get(block_nr)?.cast::<RawIdMap>();
                for r in raw.iter() {
                    if r.name != empty.name {
                        let name = byte_to_str(&r.name)?;
                        ids.ids.insert(name.into(), r.id);
                    }
                }
                db.discard(block_nr);
            }

            Ok(ids)
        }

        pub fn store(&self, db: &mut WordFileBlocks) -> Result<(), io::Error> {
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
                    db.get_mut(block_nr)?
                } else {
                    db.alloc(Self::TY)
                };
                block.set_dirty(true);
                block.discard();

                let id_list = block.cast_mut::<RawIdMap>();
                for id_rec in id_list.iter_mut() {
                    *id_rec = empty;
                    if let Some((name, id)) = it_names.next() {
                        copy_clip(name.as_bytes(), &mut id_rec.name);
                        id_rec.id = *id;
                    }
                }

                if id_list[id_list.len() - 1] == empty {
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

fn copy_fix<const LEN: usize>(src: &[u8]) -> [u8; LEN] {
    let mut dst = [0u8; LEN];
    if src.len() < LEN {
        dst[0..src.len()].copy_from_slice(src);
    } else {
        dst.copy_from_slice(&src[0..LEN]);
    }
    dst
}

fn copy_fix_left<const LEN: usize>(src: &[u8]) -> [u8; LEN] {
    let mut dst = [0u8; LEN];
    if src.len() < LEN {
        dst[0..src.len()].copy_from_slice(src);
    } else {
        let start = src.len() - LEN;
        dst.copy_from_slice(&src[start..]);
    }
    dst
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
        let path = PathBuf::from_str("tmp/init.idx")?;

        let _ = fs::remove_file(&path);

        let mut w = Words::read(&path)?;
        w.write()?;
        let w = Words::read(&path)?;
        dbg!(w);
        Ok(())
    }

    #[test]
    fn test_files() -> Result<(), AppError> {
        let path = PathBuf::from_str("tmp/files.idx")?;

        let _ = fs::remove_file(&path);

        let mut w = Words::read(&path)?;
        let _fid = w.add_file("file0".into());
        w.write()?;
        let w = Words::read(&path)?;

        assert!(w.files.list.contains_key(&1));

        Ok(())
    }

    #[test]
    fn test_files2() -> Result<(), AppError> {
        let path = PathBuf::from_str("tmp/files2.idx")?;

        let _ = fs::remove_file(&path);

        let mut w = Words::read(&path)?;
        let _fid = w.add_file("file0".into());
        let _fid = w.add_file("file1".into());
        let _fid = w.add_file("file2".into());
        let _fid = w.add_file("file3".into());
        w.write()?;
        let w = Words::read(&path)?;
        dbg!(w);

        Ok(())
    }

    #[test]
    fn test_word() -> Result<(), AppError> {
        let path = PathBuf::from_str("tmp/word.idx")?;

        let _ = fs::remove_file(&path);

        let mut w = Words::read(&path)?;
        let fid = w.add_file("file0".into());
        w.add_word("alpha", fid)?;
        w.write()?;

        let mut w = Words::read(&path)?;

        assert!(w.words.list.get("alpha").is_some());
        if let Some(word) = w.words.list.get("alpha").cloned() {
            assert_eq!(word.file_map_block_nr, 1);
            assert_eq!(word.file_map_block_idx, 0);
            assert_eq!(word.id, 1);
            let mut it = w.iter_word_files(word);
            assert_eq!(it.next().unwrap()?, 1);
            assert!(it.next().is_none());
        }

        Ok(())
    }

    #[test]
    fn test_word2() -> Result<(), AppError> {
        let path = PathBuf::from_str("tmp/word2.idx")?;

        let _ = fs::remove_file(&path);

        let mut w = Words::read(&path)?;
        let fid = w.add_file("file0".into());
        w.add_word("alpha", fid)?;
        w.add_word("beta", fid)?;
        w.add_word("gamma", fid)?;
        w.add_word("delta", fid)?;
        w.add_word("epsilon", fid)?;
        w.write()?;

        let w = Words::read(&path)?;

        assert!(w.words.list.get("alpha").is_some());
        assert!(w.words.list.get("beta").is_some());
        assert!(w.words.list.get("gamma").is_some());
        assert!(w.words.list.get("delta").is_some());
        assert!(w.words.list.get("epsilon").is_some());

        Ok(())
    }

    #[test]
    fn test_word3() -> Result<(), AppError> {
        let path = PathBuf::from_str("tmp/word3.idx")?;

        let _ = fs::remove_file(&path);

        let mut w = Words::read(&path)?;
        let fid = w.add_file("file0".into());
        w.add_word("alpha", fid)?;
        w.add_word("beta", fid)?;
        w.add_word("gamma", fid)?;
        w.add_word("delta", fid)?;
        w.add_word("epsilon", fid)?;
        let fid = w.add_file("file1".into());
        w.add_word("alpha", fid)?;
        w.add_word("beta", fid)?;
        w.add_word("gamma", fid)?;
        w.write()?;

        let mut w = Words::read(&path)?;

        assert!(w.words.list.get("alpha").is_some());
        assert!(w.words.list.get("beta").is_some());
        assert!(w.words.list.get("gamma").is_some());
        assert!(w.words.list.get("delta").is_some());
        assert!(w.words.list.get("epsilon").is_some());

        let wdata = w.words.list.get("alpha").cloned().unwrap();
        assert_eq!(wdata.file_map_block_nr, 1);
        assert_eq!(wdata.file_map_block_idx, 0);
        let mut it = w.iter_word_files(wdata);
        assert_eq!(it.next().unwrap()?, 1);
        assert_eq!(it.next().unwrap()?, 2);
        assert!(it.next().is_none());

        let wdata = w.words.list.get("beta").cloned().unwrap();
        assert_eq!(wdata.file_map_block_nr, 1);
        assert_eq!(wdata.file_map_block_idx, 1);
        let mut it = w.iter_word_files(wdata);
        assert_eq!(it.next().unwrap()?, 1);
        assert_eq!(it.next().unwrap()?, 2);
        assert!(it.next().is_none());

        Ok(())
    }

    #[test]
    fn test_word4() -> Result<(), AppError> {
        let path = PathBuf::from_str("tmp/word4.idx")?;

        let _ = fs::remove_file(&path);

        let mut w = Words::read(&path)?;
        let fid = w.add_file("file0".into());
        w.add_word("alpha", fid)?;
        w.add_word("beta", fid)?;
        w.add_word("gamma", fid)?;
        w.add_word("delta", fid)?;
        w.add_word("epsilon", fid)?;

        let _wdata = w.words.list.get("gamma").cloned().unwrap();

        let fid = w.add_file("file1".into());
        w.add_word("alpha", fid)?;
        w.add_word("beta", fid)?;
        w.add_word("gamma", fid)?;

        let _wdata = w.words.list.get("gamma").cloned().unwrap();

        for i in 0..14 {
            let fid = w.add_file(format!("file-x{}", i));
            w.add_word("gamma", fid)?;

            let _wdata = w.words.list.get("gamma").cloned().unwrap();
        }
        w.write()?;

        let mut w = Words::read(&path)?;

        let wdata = w.words.list.get("gamma").cloned().unwrap();
        let fid = w
            .iter_word_files(wdata)
            .map(|v| v.unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            fid.as_slice(),
            &[15, 16, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14]
        );

        dbg!(w.files.list);

        Ok(())
    }
}
