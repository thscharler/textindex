#![allow(dead_code)]

use crate::error::AppError;
use crate::index2::files::{FileList, RawFileList};
use crate::index2::id::{Ids, RawIdMap};
use crate::index2::word_map::{RawWordMapList, WordMap};
use crate::index2::words::{RawWordList, WordData, WordList};
use crate::tmp_index::TmpWords;
use blockfile::{BlockType, FileBlocks, UserBlockType};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Formatter};
use std::path::Path;
use std::str::from_utf8;
use std::{fs, io};
use wildmatch::WildMatch;

type BlkNr = u32;
type BlkIdx = u32;
type FIdx = u32;
type FileId = u32;
type WordId = u32;
type Id = u32;

pub struct Words {
    pub db: WordFileBlocks,
    pub ids: Ids,
    pub words: WordList,
    pub files: FileList,
    pub wordmap: WordMap,
    pub auto_save: u32,
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
    WordMapHead = BlockType::User4 as isize,
    WordMapTail = BlockType::User5 as isize,

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
            WordBlockType::WordMapHead => "WHD",
            WordBlockType::WordMapTail => "WTL",
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
            WordBlockType::WordMapHead => BlockType::User4,
            WordBlockType::WordMapTail => BlockType::User5,
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
            BlockType::User4 => Self::WordMapHead,
            BlockType::User5 => Self::WordMapTail,
            BlockType::Undefined => Self::Undefined,
            _ => Self::OtherUser,
        }
    }
}

impl Debug for Words {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Words")
            .field("ids", &self.ids)
            .field("words", &self.words)
            .field("files", &self.files)
            .field("wordmap", &self.wordmap)
            .field("db", &self.db)
            .finish()?;

        writeln!(f)?;
        for block in self.db.iter_blocks() {
            match WordBlockType::ubt(block.block_type()) {
                WordBlockType::NotAllocated => {
                    writeln!(f, "Not Allocated {}", block.block_nr())?;
                }
                WordBlockType::Free => {
                    writeln!(f, "Free {}", block.block_nr())?;
                }
                WordBlockType::BlockMap => {
                    writeln!(f, "BlockMap {}", block.block_nr())?;
                }
                WordBlockType::Ids => {
                    let data = block.cast::<RawIdMap>();
                    writeln!(f, "Ids {}", block.block_nr())?;
                    for d in data.iter() {
                        writeln!(f, "{} {}", from_utf8(&d.name).unwrap_or(""), d.id)?;
                    }
                }
                WordBlockType::WordList => {
                    let data = block.cast::<RawWordList>();
                    writeln!(f, "WordList {}", block.block_nr())?;
                    for d in data.iter() {
                        writeln!(
                            f,
                            "{} {} -> {}:{}",
                            from_utf8(&d.word).unwrap_or(""),
                            d.id,
                            d.file_map_block_nr,
                            d.file_map_idx
                        )?;
                    }
                }
                WordBlockType::FileList => {
                    let data = block.cast::<RawFileList>();
                    writeln!(f, "FileList {}", block.block_nr())?;
                    for d in data.iter() {
                        writeln!(f, "{} {}", from_utf8(&d.file).unwrap_or(""), d.id)?;
                    }
                }
                WordBlockType::WordMapHead => {
                    let data = block.cast::<RawWordMapList>();
                    writeln!(f, "WordMapHead {}", block.block_nr())?;
                    for d in data.iter() {
                        writeln!(f, "{:?} -> {} {}", d.file_id, d.next_block_nr, d.next_idx)?;
                    }
                }
                WordBlockType::WordMapTail => {
                    let data = block.cast::<RawWordMapList>();
                    writeln!(f, "WordMapTail {}", block.block_nr())?;
                    for d in data.iter() {
                        writeln!(f, "{:?} -> {} {}", d.file_id, d.next_block_nr, d.next_idx)?;
                    }
                }
                WordBlockType::OtherUser => {}
                WordBlockType::Undefined => {}
            }
        }

        Ok(())
    }
}

impl Words {
    pub fn new(file: &Path) -> Result<Self, AppError> {
        let _ = fs::remove_file(file);
        Self::read(file)
    }

    pub fn read(file: &Path) -> Result<Self, AppError> {
        let mut db = FileBlocks::open(file)?;

        let mut ids = Ids::load(&mut db)?;
        ids.create("wordmap_head");
        ids.create("wordmap_tail");
        ids.create("word");
        ids.create("word_block_nr");
        ids.create("word_block_idx");
        ids.create("file");
        ids.create("file_block_nr");
        ids.create("file_block_idx");

        let words = WordList::load(&mut db)?;
        let files = FileList::load(&mut db)?;

        let wordmap_block_nr_head = ids.get("wordmap_head");
        let wordmap_block_nr_tail = ids.get("wordmap_tail");
        let wordmap = WordMap::load(&mut db, wordmap_block_nr_head, wordmap_block_nr_tail)?;

        Ok(Self {
            db,
            ids,
            words,
            files,
            wordmap,
            auto_save: 0,
        })
    }

    pub fn reorg(&mut self) -> Result<(), AppError> {
        self.words.reorder(&mut self.db)?;
        Ok(())
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

        let (wordmap_block_nr_head, wordmap_block_nr_tail) = self.wordmap.store(&mut self.db)?;
        self.ids.set("wordmap_head", wordmap_block_nr_head);
        self.ids.set("wordmap_tail", wordmap_block_nr_tail);

        self.ids.store(&mut self.db)?;

        let mut n1 = 0;
        let mut n11 = 0;
        let mut n2 = 0;
        let mut n22 = 0;
        let mut n3 = 0;
        let mut n33 = 0;
        let mut n4 = 0;
        let mut n44 = 0;
        let mut n5 = 0;
        let mut n55 = 0;
        for block in self.db.iter_blocks() {
            if block.block_type() == BlockType::User1 {
                if block.dirty() {
                    n1 += 1;
                } else {
                    n11 += 1;
                }
            }
            if block.block_type() == BlockType::User2 {
                if block.dirty() {
                    n2 += 1;
                } else {
                    n22 += 1;
                }
            }
            if block.block_type() == BlockType::User3 {
                if block.dirty() {
                    n3 += 1;
                } else {
                    n33 += 1;
                }
            }
            if block.block_type() == BlockType::User4 {
                if block.dirty() {
                    n4 += 1;
                } else {
                    n44 += 1;
                }
            }
            if block.block_type() == BlockType::User5 {
                if block.dirty() {
                    n5 += 1;
                } else {
                    n55 += 1;
                }
            }
        }
        println!(
            "write blocks: ids {}/{} words {}/{} files {}/{} map {}/{} retired {}/{}",
            n1, n11, n2, n22, n3, n33, n4, n44, n5, n55
        );

        // println!("{:?}", &self);

        self.db.store()?;

        let mut gen_count: BTreeMap<u32, u32> = BTreeMap::new();
        for block in self.db.iter_blocks() {
            gen_count
                .entry(block.generation())
                .and_modify(|v| *v += 1)
                .or_insert_with(|| 1);
        }
        println!(
            "remain: {} generation: {:?}",
            self.db.iter_blocks().count(),
            gen_count
        );

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

    pub fn have_file(&self, txt: &String) -> bool {
        self.files.list.values().find(|v| &v.name == txt).is_some()
    }

    pub fn find_file(&self, txt: &str) -> BTreeSet<&String> {
        let find = WildMatch::new(txt);
        self.files
            .list
            .values()
            .filter(|v| find.matches(v.name.as_str()))
            .map(|v| &v.name)
            .collect()
    }

    pub fn file(&self, file_id: FileId) -> Option<String> {
        self.files.list.get(&file_id).map(|v| v.name.clone())
    }

    pub fn remove_file(&mut self, _name: String) {
        // no removes
    }

    /// Iterate words.
    pub fn iter_words(&mut self) -> impl Iterator<Item = (&String, &WordData)> {
        self.words.list.iter()
    }

    /// Iterate all files for a word.
    pub fn iter_word_files(
        &mut self,
        word_data: WordData,
    ) -> impl Iterator<Item = Result<FileId, io::Error>> + '_ {
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
            self.wordmap.add(
                &mut self.db,
                word.file_map_block_nr,
                word.file_map_block_idx,
                file_idx,
            )?;
            word.count += 1;
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
                    written: false,
                    count: 1,
                },
            );
        };
        Ok(())
    }

    pub fn append(&mut self, other: TmpWords) -> Result<(), AppError> {
        let f_idx = self.add_file(other.file);
        for a_txt in other.words.iter() {
            self.add_word(a_txt, f_idx)?;
        }
        Ok(())
    }

    pub fn find(&mut self, txt: &[&str]) -> Result<BTreeSet<String>, io::Error> {
        let mut collect = BTreeSet::<FileId>::new();
        let mut first = true;

        for t in txt {
            let match_find = WildMatch::new(t);

            let words: Vec<_> = self
                .iter_words()
                .filter(|(k, _)| match_find.matches(k))
                .map(|(_, v)| v.clone())
                .collect();

            let files = words
                .into_iter()
                .map(|v| self.iter_word_files(v).flatten().collect::<Vec<FileId>>())
                .flatten();

            if first {
                collect = files.collect();
            } else {
                collect = files.filter(|v| collect.contains(v)).collect();
            }

            first = false;
        }

        let names = collect.iter().map(|v| self.file(*v)).flatten().collect();

        Ok(names)
    }

    pub fn should_auto_save(&mut self) -> bool {
        self.auto_save += 1;
        self.auto_save % 10 == 0
    }

    pub fn should_reorg(&mut self) -> bool {
        self.auto_save % 100 == 0
    }
}

pub mod word_map {
    use crate::index2::{BlkIdx, BlkNr, FIdx, FileId, WordBlockType, WordFileBlocks};
    use blockfile::{Length, BLOCK_SIZE};
    use std::fmt::{Debug, Formatter};
    use std::io;
    use std::mem::size_of;

    pub struct WordMap {
        pub last_block_nr_head: BlkNr,
        pub last_idx_head: BlkIdx,
        pub last_block_nr_tail: BlkNr,
        pub last_idx_tail: BlkNr,
    }

    pub type RawWordMapList = [RawWordMap; BLOCK_SIZE as usize / size_of::<RawWordMap>()];

    pub const FILE_ID_LEN: usize = 4;

    #[derive(Clone, Copy, PartialEq, Default)]
    #[repr(C)]
    pub struct RawWordMap {
        pub file_id: [FileId; FILE_ID_LEN],
        pub next_block_nr: BlkNr,
        pub next_idx: BlkIdx,
    }

    impl Debug for WordMap {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("WordMap")
                .field("last_block_nr", &self.last_block_nr_head)
                .field("last_idx", &self.last_idx_head)
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
        pub const TY_LISTHEAD: WordBlockType = WordBlockType::WordMapHead;
        pub const TY_LISTTAIL: WordBlockType = WordBlockType::WordMapTail;

        pub fn load(
            db: &mut WordFileBlocks,
            last_block_nr_head: BlkNr,
            last_block_nr_tail: BlkNr,
        ) -> Result<WordMap, io::Error> {
            let empty = RawWordMap::default();

            let last_idx_head = if last_block_nr_head > 0 {
                let last = db.get(last_block_nr_head)?.cast::<RawWordMapList>();
                if let Some(empty_pos) = last.iter().position(|v| *v == empty) {
                    empty_pos as u32
                } else {
                    unreachable!();
                }
            } else {
                0u32
            };

            let last_idx_tail = if last_block_nr_tail > 0 {
                let last = db.get(last_block_nr_tail)?.cast::<RawWordMapList>();
                if let Some(empty_pos) = last.iter().position(|v| *v == empty) {
                    empty_pos as u32
                } else {
                    unreachable!();
                }
            } else {
                0u32
            };

            Ok(Self {
                last_block_nr_head,
                last_idx_head,
                last_block_nr_tail,
                last_idx_tail,
            })
        }

        pub fn store(&mut self, _db: &mut WordFileBlocks) -> Result<(BlkNr, BlkNr), io::Error> {
            Ok((self.last_block_nr_head, self.last_block_nr_tail))
        }

        // Ensures we can add at least 1 new region.
        fn ensure_add_head(&mut self, db: &mut WordFileBlocks) -> BlkIdx {
            let res_idx = if self.last_block_nr_head == 0 {
                let new_block_nr = db.alloc(Self::TY_LISTHEAD).block_nr();

                self.last_block_nr_head = new_block_nr;
                self.last_idx_head = 0;

                self.last_idx_head
            } else {
                if self.last_idx_head + 1 >= RawWordMapList::LEN as u32 {
                    let new_block_nr = db.alloc(Self::TY_LISTHEAD).block_nr();

                    self.last_block_nr_head = new_block_nr;
                    self.last_idx_head = 0;

                    self.last_idx_head
                } else {
                    self.last_idx_head + 1
                }
            };

            res_idx
        }

        // Ensures we can add at least 1 new region.
        fn ensure_add_tail(&mut self, db: &mut WordFileBlocks) -> BlkIdx {
            let res_idx = if self.last_block_nr_tail == 0 {
                let new_block_nr = db.alloc(Self::TY_LISTTAIL).block_nr();

                self.last_block_nr_tail = new_block_nr;
                self.last_idx_tail = 0;

                self.last_idx_tail
            } else {
                if self.last_idx_tail + 1 >= RawWordMapList::LEN as u32 {
                    let new_block_nr = db.alloc(Self::TY_LISTTAIL).block_nr();

                    self.last_block_nr_tail = new_block_nr;
                    self.last_idx_tail = 0;

                    self.last_idx_tail
                } else {
                    self.last_idx_tail + 1
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
            let new_idx = self.ensure_add_head(db);

            let block = db.get_mut(self.last_block_nr_head)?;
            block.set_dirty(true);

            let last = block.cast_mut::<RawWordMapList>();
            last[new_idx as usize].file_id[0] = file_id;

            self.last_idx_head = new_idx;

            Ok((self.last_block_nr_head, self.last_idx_head))
        }

        /// Add one more file reference for a word.
        pub fn add(
            &mut self,
            db: &mut WordFileBlocks,
            blk_nr: BlkNr,
            blk_idx: BlkIdx,
            file_id: FileId,
        ) -> Result<(), io::Error> {
            // append to given region list.
            {
                let retire_idx = self.ensure_add_tail(db);
                let retire_block_nr = self.last_block_nr_tail;

                let block = db.get_mut(blk_nr)?;
                block.set_dirty(true);
                let word_map_list = block.cast_mut::<RawWordMapList>();
                let word_map = &mut word_map_list[blk_idx as usize];

                if let Some(insert_pos) = word_map.file_id.iter().position(|v| *v == 0) {
                    word_map.file_id[insert_pos] = file_id;
                } else {
                    // move out of current
                    let retire_file_id = word_map.file_id;
                    let retire_next_block_nr = word_map.next_block_nr;
                    let retire_next_idx = word_map.next_idx;

                    // re-init and write
                    word_map.file_id = [0u32; FILE_ID_LEN];
                    word_map.next_block_nr = retire_block_nr;
                    word_map.next_idx = retire_idx;
                    word_map.file_id[0] = file_id;

                    // retire
                    let retire_block = db.get_mut(self.last_block_nr_tail)?;
                    retire_block.set_dirty(true);
                    let retire_map_list = retire_block.cast_mut::<RawWordMapList>();
                    let retire_map = &mut retire_map_list[retire_idx as usize];

                    retire_map.file_id = retire_file_id;
                    retire_map.next_block_nr = retire_next_block_nr;
                    retire_map.next_idx = retire_next_idx;
                }
            }
            Ok(())
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

            let mut _discard_block = None;
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
                        _discard_block = Some(self.map_block_nr);
                        self.map_block_nr = map.next_block_nr;
                        self.map_idx = map.next_idx;
                        self.file_idx = 0;
                    }
                    break Some(file_id);
                } else {
                    if map.next_block_nr != 0 {
                        _discard_block = Some(self.map_block_nr);
                        self.map_block_nr = map.next_block_nr;
                        self.map_idx = map.next_idx;
                        self.file_idx = 0;
                    } else {
                        break None;
                    }
                }
            };

            // if let Some(block_nr) = discard_block {
            //     self.db.discard(block_nr);
            // }

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
        pub file: [u8; 124],
        pub id: FileId,
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
    use std::fmt::{Debug, Formatter};
    use std::io;
    use std::mem::size_of;
    use std::str::from_utf8;

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
        pub written: bool,
        pub count: u32,
    }

    pub type RawWordList = [RawWord; BLOCK_SIZE as usize / size_of::<RawWord>()];

    #[derive(Clone, Copy, PartialEq)]
    #[repr(C)]
    pub struct RawWord {
        pub word: [u8; 20],
        pub id: WordId,
        pub file_map_block_nr: BlkNr,
        pub file_map_idx: BlkIdx,
    }

    impl Debug for RawWord {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            let w = from_utf8(&self.word).unwrap_or("");
            write!(
                f,
                "{} {} -> {} {}",
                w, self.id, self.file_map_block_nr, self.file_map_idx
            )
        }
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
                                written: false,
                                count: 0,
                            },
                        );
                    }
                }
                db.discard(block_nr);
            }

            Ok(words)
        }

        /// Reorder the word list due to histogram data.
        /// Does not store to disk.
        pub fn reorder(&mut self, db: &mut WordFileBlocks) -> Result<(), io::Error> {
            let mut reorder: Vec<_> = self.list.values_mut().map(|w| (w.count, w)).collect();
            reorder.sort_by(|v, w| w.0.cmp(&v.0));

            let mut n: u32 = 0;

            let mut it = reorder.into_iter();
            let blocks: Vec<_> = db
                .iter_metadata()
                .filter(|v| v.1 == Self::TY)
                .map(|v| v.0)
                .collect();
            for block_nr in blocks {
                for idx in 0..RawWordList::LEN {
                    if let Some((_, data)) = it.next() {
                        if data.block_nr != block_nr || data.block_idx != idx as u32 {
                            n += 1;
                        }
                        data.block_nr = block_nr;
                        data.block_idx = idx as u32;
                    }
                }
            }
            println!("reorder {}/{}", n, self.list.len());

            Ok(())
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
                        word_data.written = true;
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
                    word_data.written = true;

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
        pub name: [u8; 28],
        pub id: Id,
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
    use blockfile::Length;
    use blockfile::BLOCK_SIZE;
    use std::fs;
    use std::mem::size_of;
    use std::path::PathBuf;
    use std::str::FromStr;

    #[test]
    fn test_sizes() {
        println!("RawWordmapList {}", size_of::<RawWordMapList>());
        println!("RawWordmapList::LEN {}", RawWordMapList::LEN);
        println!("RawWordMap {}", size_of::<RawWordMap>());
        println!("RawFileList {}", size_of::<RawFileList>());
        println!("RawFileList::LEN {}", RawFileList::LEN);
        println!("RawFile {}", size_of::<RawFile>());
        println!("RawWordList {}", size_of::<RawWordList>());
        println!("RawWordList::LEN {}", RawWordList::LEN);
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
        {
            let mut it = w.iter_word_files(wdata);
            assert_eq!(it.next().unwrap()?, 1);
            assert_eq!(it.next().unwrap()?, 2);
            assert!(it.next().is_none());
        }

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

        // dbg!(w);

        Ok(())
    }
}
