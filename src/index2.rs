#![allow(dead_code)]

use crate::error::AppError;
use crate::index2::files::FileList;
use crate::index2::id::{Ids, RawIdMap};
use crate::index2::word_map::{RawWordMapList, WordMap};
use crate::index2::words::{RawWordList, WordData, WordList};
use crate::tmp_index::TmpWords;
use blockfile::{BlockType, FileBlocks, UserBlockType};
use std::collections::BTreeSet;
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
        if f.width().unwrap_or(0) == 0 {
            f.debug_struct("Words")
                .field("ids", &self.ids)
                .field("words", &self.words.list.len())
                .field("files", &self.files.list.len())
                .field("wordmap", &self.wordmap)
                .field("db", &self.db)
                .finish()?;
        } else if f.width().unwrap_or(0) >= 1 {
            f.debug_struct("Words")
                .field("ids", &self.ids)
                .field("words", &self.words)
                .field("files", &self.files)
                .field("wordmap", &self.wordmap)
                .field("db", &self.db)
                .finish()?;
        }

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
                    if f.width().unwrap_or(0) >= 1 {
                        for d in data.iter() {
                            writeln!(
                                f,
                                "{} {} -> {}:{}",
                                from_utf8(&d.word).unwrap_or(""),
                                d.id,
                                d.file_map_block_nr,
                                d.file_map_idx_or_file_id
                            )?;
                        }
                    }
                }
                WordBlockType::FileList => {
                    writeln!(f, "FileList {}", block.block_nr())?;
                    if f.width().unwrap_or(0) >= 1 {
                        writeln!(f, "{:?}", block)?;
                    }
                }
                WordBlockType::WordMapHead => {
                    let data = block.cast::<RawWordMapList>();
                    writeln!(f, "WordMapHead {}", block.block_nr())?;
                    if f.width().unwrap_or(0) >= 1 {
                        for d in data.iter() {
                            writeln!(f, "{:?} -> {} {}", d.file_id, d.next_block_nr, d.next_idx)?;
                        }
                    }
                }
                WordBlockType::WordMapTail => {
                    let data = block.cast::<RawWordMapList>();
                    writeln!(f, "WordMapTail {}", block.block_nr())?;
                    if f.width().unwrap_or(0) >= 1 {
                        for d in data.iter() {
                            writeln!(f, "{:?} -> {} {}", d.file_id, d.next_block_nr, d.next_idx)?;
                        }
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
        ids.store(&mut db)?;

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

    /// Write everything to FileBlocks but don't actually store anything.
    pub fn store(&mut self) -> Result<(), AppError> {
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
        Ok(())
    }

    pub fn write(&mut self) -> Result<(), AppError> {
        self.store()?;

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
            word_data.file_map_idx,
            word_data.first_file_id,
        )
    }

    /// Add a word and a file reference.
    /// It is not checked, if the reference was already inserted.
    /// Duplicates are acceptable.
    pub fn add_word<S: AsRef<str> + Into<String>>(
        &mut self,
        word: S,
        file_id: FileId,
    ) -> Result<(), AppError> {
        if let Some(data) = self.words.list.get_mut(word.as_ref()) {
            // first file-id is stored directly with the word. this covers a surprisingly
            // large number of cases.
            if data.first_file_id != 0 {
                let (file_map_block_nr, file_map_idx) =
                    self.wordmap
                        .add_initial(&mut self.db, word.as_ref(), file_id)?;

                data.first_file_id = 0;
                data.file_map_block_nr = file_map_block_nr;
                data.file_map_idx = file_map_idx;
            }

            // add second file-id. (and any further).
            self.wordmap.add(
                &mut self.db,
                word.as_ref(),
                data.file_map_block_nr,
                data.file_map_idx,
                file_id,
            )?;
        } else {
            let word_id = self.ids.next("word");

            self.words.list.insert(
                word.as_ref().into(),
                WordData {
                    id: word_id,
                    block_nr: 0,
                    block_idx: 0,
                    file_map_block_nr: 0,
                    file_map_idx: 0,
                    first_file_id: file_id,
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
        self.auto_save % 1000 == 0
    }

    pub fn should_reorg(&mut self) -> bool {
        self.auto_save % 1000 == 0
    }
}

pub mod word_map {
    use crate::index2::{BlkIdx, BlkNr, FIdx, FileId, WordBlockType, WordFileBlocks};
    use blockfile::{Length, BLOCK_SIZE};
    use std::fmt::{Debug, Formatter};
    use std::io;
    use std::mem::size_of;

    #[derive(Debug)]
    pub struct WordMap {
        pub last_block_nr_head: BlkNr,
        pub last_idx_head: BlkIdx,
        pub last_block_nr_tail: BlkNr,
        pub last_idx_tail: BlkNr,
    }

    pub type RawWordMapList = [RawWordMap; BLOCK_SIZE as usize / size_of::<RawWordMap>()];

    pub const FILE_ID_LEN: usize = 6;

    #[derive(Clone, Copy, PartialEq, Default)]
    #[repr(C)]
    pub struct RawWordMap {
        pub file_id: [FileId; FILE_ID_LEN],
        pub next_block_nr: BlkNr,
        pub next_idx: BlkIdx,
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

        fn confirm_add_head(&mut self, last_block_nr_head: BlkNr, last_idx_head: BlkIdx) {
            self.last_block_nr_head = last_block_nr_head;
            self.last_idx_head = last_idx_head;
        }

        // Ensures we can add at least 1 new region.
        fn ensure_add_head(&mut self, db: &mut WordFileBlocks) -> (BlkNr, BlkIdx) {
            if self.last_block_nr_head == 0 {
                let new_block_nr = db.alloc(Self::TY_LISTHEAD).block_nr();

                self.last_block_nr_head = new_block_nr;
                self.last_idx_head = 0;

                (self.last_block_nr_head, self.last_idx_head)
            } else {
                if self.last_idx_head + 1 >= RawWordMapList::LEN as u32 {
                    let new_block_nr = db.alloc(Self::TY_LISTHEAD).block_nr();

                    self.last_block_nr_head = new_block_nr;
                    self.last_idx_head = 0;

                    (self.last_block_nr_head, self.last_idx_head)
                } else {
                    (self.last_block_nr_head, self.last_idx_head + 1)
                }
            }
        }

        fn confirm_add_tail(&mut self, last_block_nr_tail: BlkNr, last_idx_tail: BlkIdx) {
            self.last_block_nr_tail = last_block_nr_tail;
            self.last_idx_tail = last_idx_tail;
        }

        // Ensures we can add at least 1 new region.
        fn ensure_add_tail(&mut self, db: &mut WordFileBlocks) -> (BlkNr, BlkIdx) {
            if self.last_block_nr_tail == 0 {
                let new_block_nr = db.alloc(Self::TY_LISTTAIL).block_nr();

                self.last_block_nr_tail = new_block_nr;
                self.last_idx_tail = 0;

                (self.last_block_nr_tail, self.last_idx_tail)
            } else {
                if self.last_idx_tail + 1 >= RawWordMapList::LEN as u32 {
                    let new_block_nr = db.alloc(Self::TY_LISTTAIL).block_nr();

                    self.last_block_nr_tail = new_block_nr;
                    self.last_idx_tail = 0;

                    (self.last_block_nr_tail, self.last_idx_tail)
                } else {
                    (self.last_block_nr_tail, self.last_idx_tail + 1)
                }
            }
        }

        /// Add first reference for a new word.
        pub fn add_initial(
            &mut self,
            db: &mut WordFileBlocks,
            _word: &str,
            file_id: FileId,
        ) -> Result<(BlkNr, BlkIdx), io::Error> {
            let (new_blk_nr, new_idx) = self.ensure_add_head(db);

            let block = db.get_mut(new_blk_nr)?;
            block.set_dirty(true);
            let word_map_list = block.cast_mut::<RawWordMapList>();
            let word_map = &mut word_map_list[new_idx as usize];

            word_map.file_id[0] = file_id;

            self.confirm_add_head(new_blk_nr, new_idx);

            Ok((new_blk_nr, new_idx))
        }

        /// Add one more file reference for a word.
        pub fn add(
            &mut self,
            db: &mut WordFileBlocks,
            _word: &str,
            blk_nr: BlkNr,
            blk_idx: BlkIdx,
            file_id: FileId,
        ) -> Result<(), io::Error> {
            // append to given region list.
            {
                let (retire_block_nr, retire_idx) = self.ensure_add_tail(db);

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
                    retire_block.discard();
                    let retire_map_list = retire_block.cast_mut::<RawWordMapList>();
                    let retire_map = &mut retire_map_list[retire_idx as usize];

                    retire_map.file_id = retire_file_id;
                    retire_map.next_block_nr = retire_next_block_nr;
                    retire_map.next_idx = retire_next_idx;

                    self.confirm_add_tail(retire_block_nr, retire_idx);
                }
            }
            Ok(())
        }

        pub fn iter_files(
            db: &mut WordFileBlocks,
            block_nr: BlkNr,
            block_idx: BlkIdx,
            first_file_id: FileId,
        ) -> IterFileId {
            IterFileId {
                db,
                first_file_id: first_file_id,
                map_block_nr: block_nr,
                map_idx: block_idx,
                file_idx: 0,
            }
        }
    }

    pub struct IterFileId<'a> {
        db: &'a mut WordFileBlocks,
        first_file_id: FileId,
        map_block_nr: BlkNr,
        map_idx: BlkIdx,
        file_idx: FIdx,
    }

    impl<'a> IterFileId<'a> {
        fn is_clear(&self) -> bool {
            self.map_block_nr == 0 && self.first_file_id == 0
        }

        fn clear(&mut self) {
            self.first_file_id = 0;
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

            if self.first_file_id != 0 {
                let first_file_id = self.first_file_id;
                self.clear();
                return Some(Ok(first_file_id));
            }

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
                        self.map_block_nr = map.next_block_nr;
                        self.map_idx = map.next_idx;
                        self.file_idx = 0;
                    }
                    break Some(file_id);
                } else {
                    if map.next_block_nr != 0 {
                        self.map_block_nr = map.next_block_nr;
                        self.map_idx = map.next_idx;
                        self.file_idx = 0;
                    } else {
                        break None;
                    }
                }
            };

            file_id.map(|v| Ok(v))
        }
    }
}

pub mod files {
    use crate::index2::{BlkIdx, BlkNr, FileId, WordBlockType, WordFileBlocks};
    use blockfile::BLOCK_SIZE;
    use std::collections::BTreeMap;
    use std::fmt::Debug;
    use std::io;

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

    // // pseudo array ...
    // pub type RawFileList = [u8; BLOCK_SIZE as usize];
    //
    // // pseudo struct ...
    // pub struct RawFile {
    //     pub id: FileId,
    //     pub len: u8,
    //     pub file: [u8],
    // }

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

            for block_nr in blocks {
                let block = db.get(block_nr)?;
                let mut idx = 0usize;

                'f: loop {
                    if idx + 4 >= block.raw.len() {
                        break 'f;
                    }
                    let mut file_id = [0u8; 4];
                    file_id.copy_from_slice(&block.raw[idx..idx + 4]);
                    let file_id = FileId::from_ne_bytes(file_id);
                    if file_id == 0 {
                        break 'f;
                    }
                    let name_len = block.raw[idx + 4] as usize;
                    let name = &block.raw[idx + 5..idx + 5 + name_len];

                    files.list.insert(
                        file_id,
                        FileData {
                            name: String::from_utf8_lossy(name).into(),
                            block_nr,
                            block_idx: idx as BlkIdx,
                        },
                    );

                    idx += 5 + name_len;
                }
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

                    assert!(file_data.name.len() < 256);

                    let file_name = file_data.name.as_bytes();

                    let mut buf: Vec<u8> = Vec::new();
                    buf.extend(file_id.to_ne_bytes());
                    buf.extend((file_name.len() as u8).to_ne_bytes());
                    buf.extend(file_name);

                    let mut block = db.get_mut(*last_block_nr)?;
                    let mut idx = *last_block_idx as usize;
                    if idx + buf.len() > BLOCK_SIZE as usize {
                        block = db.alloc(Self::TY);
                        *last_block_nr = block.block_nr();
                        *last_block_idx = 0;
                        idx = 0;
                    }
                    block.set_dirty(true);
                    block.discard();

                    let raw_buf = block.raw.get_mut(idx..idx + buf.len()).expect("buffer");
                    raw_buf.copy_from_slice(buf.as_slice());

                    file_data.block_nr = *last_block_nr;
                    file_data.block_idx = *last_block_idx;

                    *last_block_idx += buf.len() as u32;
                } else {
                    // no updates
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
        byte_to_str, copy_fix, BlkIdx, BlkNr, FileId, WordBlockType, WordFileBlocks, WordId,
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
        pub file_map_idx: BlkIdx,
        pub first_file_id: FileId,
    }

    pub type RawWordList = [RawWord; BLOCK_SIZE as usize / size_of::<RawWord>()];

    #[derive(Clone, Copy, PartialEq)]
    #[repr(C)]
    pub struct RawWord {
        pub word: [u8; 20],
        pub id: WordId,
        pub file_map_block_nr: BlkNr,
        pub file_map_idx_or_file_id: BlkIdx,
    }

    impl Debug for RawWord {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            let w = from_utf8(&self.word).unwrap_or("");
            write!(
                f,
                "{} {} -> {} {}",
                w, self.id, self.file_map_block_nr, self.file_map_idx_or_file_id
            )
        }
    }

    impl Default for RawWord {
        fn default() -> Self {
            Self {
                word: Default::default(),
                id: 0,
                file_map_block_nr: 0,
                file_map_idx_or_file_id: 0,
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
                        // block_nr == 0 means we have only one file-id and it is stored
                        // as file_map_idx.
                        if r.file_map_block_nr == 0 {
                            words.list.insert(
                                word.into(),
                                WordData {
                                    id: r.id,
                                    block_nr,
                                    block_idx: i as u32,
                                    file_map_block_nr: 0,
                                    file_map_idx: 0,
                                    first_file_id: r.file_map_idx_or_file_id,
                                },
                            );
                        } else {
                            words.list.insert(
                                word.into(),
                                WordData {
                                    id: r.id,
                                    block_nr,
                                    block_idx: i as u32,
                                    file_map_block_nr: r.file_map_block_nr,
                                    file_map_idx: r.file_map_idx_or_file_id,
                                    first_file_id: 0,
                                },
                            );
                        }
                    }
                }
                db.discard(block_nr);
            }

            Ok(words)
        }

        /// Reorder the word list due to histogram data.
        /// Does not store to disk.
        pub fn reorder(&mut self, _db: &mut WordFileBlocks) -> Result<(), io::Error> {
            // the linked list is created within the word-map, instead of always
            // rewriting the head here. keeps the necessary writes mostly to the
            // head-blocks and newly added words.
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
                let w = if word_data.first_file_id != 0 {
                    RawWord {
                        word: copy_fix::<20>(word.as_bytes()),
                        id: word_data.id,
                        file_map_block_nr: 0,
                        file_map_idx_or_file_id: word_data.first_file_id,
                    }
                } else {
                    RawWord {
                        word: copy_fix::<20>(word.as_bytes()),
                        id: word_data.id,
                        file_map_block_nr: word_data.file_map_block_nr,
                        file_map_idx_or_file_id: word_data.file_map_idx,
                    }
                };

                if word_data.block_nr != 0 {
                    let block = db.get_mut(word_data.block_nr)?;
                    let word_list = block.cast_mut::<RawWordList>();

                    if word_list[word_data.block_idx as usize] != w {
                        word_list[word_data.block_idx as usize] = w;
                        block.set_dirty(true);
                        block.discard();
                    }
                } else {
                    if *last_block_nr == 0 {
                        *last_block_nr = db.alloc(Self::TY).block_nr();
                        *last_block_idx = 0;
                    }

                    let block = db.get_mut(*last_block_nr)?;
                    block.set_dirty(true);
                    block.discard();
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

            let mut it_names = self.ids.iter();
            'f: loop {
                let block = if let Some(block_nr) = blocks.pop() {
                    db.get_mut(block_nr)?
                } else {
                    db.alloc(Self::TY)
                };
                block.clear();
                block.set_dirty(true);
                block.discard();

                let id_list = block.cast_mut::<RawIdMap>();
                for id_rec in id_list.iter_mut() {
                    if let Some((name, id)) = it_names.next() {
                        copy_clip(name.as_bytes(), &mut id_rec.name);
                        id_rec.id = *id;
                    } else {
                        break 'f;
                    }
                }
            }
            for block_nr in blocks {
                db.free(block_nr)?;
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
