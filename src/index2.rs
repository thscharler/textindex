#![allow(dead_code)]

use crate::index2::files::{FileData, FileList};
use crate::index2::word_map::{RawWordMapList, WordMap};
use crate::index2::words::{RawWordList, WordData, WordList};
use crate::tmp_index::TmpWords;
use blockfile2::{BlockType, FileBlocks, UserBlockType};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Display, Formatter};
use std::fs;
use std::mem::align_of;
use std::path::Path;
use std::str::from_utf8;
use wildmatch::WildMatch;

type BlkIdx = u32;
type FIdx = u32;
type FileId = u32;
type WordId = u32;
type Id = u32;

#[derive(Debug)]
pub enum IndexError {
    BlockFile(blockfile2::Error),
    Utf8Error(Vec<u8>),
}

impl Display for IndexError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            IndexError::BlockFile(e) => write!(f, "BlockFile {:?}", e),
            IndexError::Utf8Error(v) => write!(f, "Utf8Error {:?}", v),
        }
    }
}

impl From<blockfile2::Error> for IndexError {
    fn from(value: blockfile2::Error) -> Self {
        IndexError::BlockFile(value)
    }
}

impl std::error::Error for IndexError {}

const BLOCK_SIZE: usize = 4096;

pub struct Words {
    db: WordFileBlocks,
    words: WordList,
    files: FileList,
    wordmap: WordMap,
    auto_save: u32,
}

pub type WordFileBlocks = FileBlocks<WordBlockType>;

#[derive(Clone, Copy, PartialEq)]
pub enum WordBlockType {
    WordList = BlockType::User1 as isize,
    FileList = BlockType::User2 as isize,
    WordMapHead = BlockType::User3 as isize,
    WordMapTail = BlockType::User4 as isize,
}

impl TryFrom<u8> for WordBlockType {
    type Error = u8;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            16 => Ok(WordBlockType::WordList),
            17 => Ok(WordBlockType::FileList),
            18 => Ok(WordBlockType::WordMapHead),
            19 => Ok(WordBlockType::WordMapTail),
            _ => Err(value),
        }
    }
}

impl Display for WordBlockType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Debug for WordBlockType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let v = match self {
            WordBlockType::WordList => "WRD",
            WordBlockType::FileList => "FIL",
            WordBlockType::WordMapHead => "WHD",
            WordBlockType::WordMapTail => "WTL",
        };
        write!(f, "{}", v)
    }
}

impl UserBlockType for WordBlockType {
    fn block_type(self) -> BlockType {
        match self {
            WordBlockType::WordList => BlockType::User2,
            WordBlockType::FileList => BlockType::User3,
            WordBlockType::WordMapHead => BlockType::User4,
            WordBlockType::WordMapTail => BlockType::User5,
        }
    }

    fn user_type(block_type: BlockType) -> Option<Self> {
        match block_type {
            BlockType::User2 => Some(Self::WordList),
            BlockType::User3 => Some(Self::FileList),
            BlockType::User4 => Some(Self::WordMapHead),
            BlockType::User5 => Some(Self::WordMapTail),
            _ => None,
        }
    }

    fn align(self) -> usize {
        match self {
            WordBlockType::WordList => align_of::<RawWordList>(),
            WordBlockType::FileList => align_of::<[u8; 0]>(),
            WordBlockType::WordMapHead => align_of::<RawWordMapList>(),
            WordBlockType::WordMapTail => align_of::<RawWordMapList>(),
        }
    }
}

impl Debug for Words {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if f.width().unwrap_or(0) == 0 {
            f.debug_struct("Words")
                .field("words", &self.words.len())
                .field("files", &self.files.len())
                .field("wordmap", &self.wordmap)
                .field("db", &self.db)
                .finish()?;
        } else if f.width().unwrap_or(0) >= 1 {
            f.debug_struct("Words")
                .field("words", &self.words)
                .field("files", &self.files)
                .field("wordmap", &self.wordmap)
                .field("db", &self.db)
                .finish()?;
        }

        writeln!(f)?;
        for block in self.db.iter_blocks() {
            match WordBlockType::user_type(block.block_type()) {
                Some(WordBlockType::WordList) => {
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
                Some(WordBlockType::FileList) => {
                    writeln!(f, "FileList {}", block.block_nr())?;
                    if f.width().unwrap_or(0) >= 1 {
                        writeln!(f, "{:?}", block)?;
                    }
                }
                Some(WordBlockType::WordMapHead) => {
                    let data = block.cast::<RawWordMapList>();
                    writeln!(f, "WordMapHead {}", block.block_nr())?;
                    if f.width().unwrap_or(0) >= 1 {
                        for d in data.iter() {
                            writeln!(f, "{:?} -> {} {}", d.file_id, d.next_block_nr, d.next_idx)?;
                        }
                    }
                }
                Some(WordBlockType::WordMapTail) => {
                    let data = block.cast::<RawWordMapList>();
                    writeln!(f, "WordMapTail {}", block.block_nr())?;
                    if f.width().unwrap_or(0) >= 1 {
                        for d in data.iter() {
                            writeln!(f, "{:?} -> {} {}", d.file_id, d.next_block_nr, d.next_idx)?;
                        }
                    }
                }
                None => {
                    writeln!(f, "{:?} {}", block.block_type(), block.block_nr())?;
                }
            }
        }

        Ok(())
    }
}

pub(crate) struct LastRef {
    pub id: u32,
    pub block_nr: u32,
    pub block_idx: u32,
}

impl Words {
    pub fn create(file: &Path) -> Result<Self, IndexError> {
        let _ = fs::remove_file(file);
        Self::read(file)
    }

    pub fn read(file: &Path) -> Result<Self, IndexError> {
        // 382_445 Dateien, 16_218 Ordner
        // 8,56 GB (9_194_861_782 Bytes)

        let mut db = match FileBlocks::load(file, BLOCK_SIZE) {
            Ok(db) => db,
            Err(err) => {
                println!("{:?}", err);
                return Err(err.into());
            }
        };

        // match db.recovered_state() {
        //     FileBlockState::Consistent => {
        println!("load files");
        let files = FileList::load(&mut db)?;

        println!("load words");
        let words = WordList::load(&mut db)?;

        println!("load wordmap");
        let wordmap = WordMap::load(&mut db)?;

        Ok(Self {
            db,
            words,
            files,
            wordmap,
            auto_save: 0,
        })
        //     }
        //     _ => {
        //         println!("recover files");
        //         let files = FileList::recover(&mut db)?;
        //
        //         println!("recover words");
        //         let mut words = WordList::recover(&mut db, files.last_file_id())?;
        //
        //         println!("recover wordmap");
        //         let wordmap = WordMap::recover(&mut words, &files, &mut db)?;
        //
        //         db.recover_complete()?;
        //
        //         Ok(Self {
        //             db,
        //             words,
        //             files,
        //             wordmap,
        //             auto_save: 0,
        //         })
        //     }
        // }
    }

    /// Write everything to FileBlocks but don't actually store anything.
    pub fn store_to_db(&mut self) -> Result<(), IndexError> {
        self.words.store(&mut self.db)?;
        self.files.store(&mut self.db)?;
        self.wordmap.store(&mut self.db)?;
        Ok(())
    }

    pub fn write(&mut self) -> Result<(), IndexError> {
        self.store_to_db()?;
        self.write_stats();
        self.db.store()?;
        self.cleanup()?;
        Ok(())
    }

    pub fn compact_blocks(&mut self) {
        // todo: self.db.compact_to()
    }

    fn cleanup(&mut self) -> Result<(), IndexError> {
        let generation = self.db.generation();

        // retain some datablocks in memory.
        self.db
            .retain(|_k, v| match WordBlockType::user_type(v.block_type()) {
                Some(WordBlockType::WordList) => v.generation() + 2 >= generation,
                Some(WordBlockType::FileList) => false,
                Some(WordBlockType::WordMapHead) => true,
                Some(WordBlockType::WordMapTail) => v.generation() + 2 >= generation,
                None => false, // doesn't matter
            });
        Ok(())
    }

    fn write_stats(&mut self) {
        let mut dirty = [0u32; 32];
        let mut clean = [0u32; 32];
        for block in self.db.iter_blocks() {
            if block.is_dirty() {
                dirty[block.block_type() as usize] += 1;
            } else {
                clean[block.block_type() as usize] += 1;
            }
        }
        for block in self.db.iter_physical() {
            if block.is_dirty() {
                dirty[BlockType::Physical as usize] += 1;
            } else {
                clean[BlockType::Physical as usize] += 1;
            }
        }
        for block in self.db.iter_types() {
            if block.is_dirty() {
                dirty[BlockType::Types as usize] += 1;
            } else {
                clean[BlockType::Types as usize] += 1;
            }
        }
        print!(
            "write {} words {} files: ",
            self.words.len(),
            self.files.len()
        );
        for i in 0..32 {
            if dirty[i] > 0 || clean[i] > 0 {
                print!(
                    "{} {}/{} ",
                    match WordBlockType::try_from(i as u8) {
                        Ok(v) => v.to_string(),
                        Err(e) => e.to_string(),
                    },
                    dirty[i],
                    clean[i]
                );
            }
        }
        println!();
    }

    /// Adds a new file.
    /// It's not checked, if the same file was already added.
    /// Simply returns a new FileId.
    pub fn add_file(&mut self, file: String) -> FileId {
        self.files.add(file)
    }

    pub fn have_file(&self, txt: &String) -> bool {
        self.files.list().values().any(|v| &v.name == txt)
    }

    pub fn files(&self) -> &BTreeMap<FileId, FileData> {
        self.files.list()
    }

    pub fn words(&self) -> &BTreeMap<String, WordData> {
        self.words.list()
    }

    pub fn find_file(&self, txt: &str) -> BTreeSet<&String> {
        let find = WildMatch::new(txt);
        self.files
            .list()
            .values()
            .filter(|v| find.matches(v.name.as_str()))
            .map(|v| &v.name)
            .collect()
    }

    pub fn file(&self, file_id: FileId) -> Option<String> {
        self.files.list().get(&file_id).map(|v| v.name.clone())
    }

    pub fn remove_file(&mut self, _name: String) {
        // todo: no removes
    }

    /// Iterate words.
    pub fn iter_words(&mut self) -> impl Iterator<Item = (&String, &WordData)> {
        self.words.iter_words()
    }

    /// Iterate all files for a word.
    pub fn iter_word_files(
        &mut self,
        word_data: WordData,
    ) -> impl Iterator<Item = Result<FileId, IndexError>> + '_ {
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
    pub fn add_word<S: AsRef<str>>(&mut self, word: S, file_id: FileId) -> Result<(), IndexError> {
        if let Some(data) = self.words.get_mut(word.as_ref()) {
            if data.first_file_id == 0 && data.file_map_block_nr == 0 {
                // Recovery lost the file refs.
                data.first_file_id = file_id;
            } else {
                // first file-id is stored directly with the word. this covers a surprisingly
                // large number of cases.
                if data.first_file_id != 0 {
                    let (file_map_block_nr, file_map_idx) = self.wordmap.add_initial(
                        &mut self.db,
                        word.as_ref(),
                        data.first_file_id,
                    )?;

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
            }
        } else {
            self.words.insert(word, file_id);
        };
        Ok(())
    }

    /// Append a temp buffer for a file.
    pub fn append(&mut self, other: TmpWords) -> Result<(), IndexError> {
        let f_idx = self.add_file(other.file);
        for a_txt in other.words.iter() {
            self.add_word(a_txt, f_idx)?;
        }
        Ok(())
    }

    /// Find words.
    pub fn find(&mut self, txt: &[&str]) -> Result<BTreeSet<String>, IndexError> {
        let mut collect = BTreeSet::<FileId>::new();
        let mut first = true;

        for t in txt {
            let match_find = WildMatch::new(t);

            let words: Vec<_> = self
                .iter_words()
                .filter(|(k, _)| match_find.matches(k))
                .map(|(_, v)| *v)
                .collect();

            let files = words
                .into_iter()
                .flat_map(|v| self.iter_word_files(v).flatten().collect::<Vec<FileId>>());

            if first {
                collect = files.collect();
            } else {
                collect = files.filter(|v| collect.contains(v)).collect();
            }

            first = false;
        }

        let names = collect.iter().flat_map(|v| self.file(*v)).collect();

        Ok(names)
    }

    pub fn should_auto_save(&mut self) -> bool {
        self.auto_save += 1;
        self.auto_save % 1000 == 0
    }
}

pub mod word_map {
    use crate::index2::{
        BlkIdx, FIdx, FileId, IndexError, WordBlockType, WordFileBlocks, BLOCK_SIZE,
    };
    use blockfile2::{Length, LogicalNr};
    use std::cmp::max;
    use std::fmt::{Debug, Formatter};
    use std::mem::size_of;

    #[derive(Debug)]
    pub struct WordMap {
        last_block_nr_head: LogicalNr,
        last_idx_head: BlkIdx,
        last_block_nr_tail: LogicalNr,
        last_idx_tail: BlkIdx,
    }

    pub type RawWordMapList = [RawWordMap; BLOCK_SIZE / size_of::<RawWordMap>()];

    pub const FILE_ID_LEN: usize = 6;

    #[derive(Clone, Copy, PartialEq, Default)]
    #[repr(C)]
    pub struct RawWordMap {
        pub file_id: [FileId; FILE_ID_LEN],
        pub next_block_nr: LogicalNr,
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

        // pub fn recover(
        //     words: &mut WordList,
        //     files: &FileList,
        //     db: &mut WordFileBlocks,
        // ) -> Result<WordMap, IndexError> {
        //     for (word, data) in words.list_mut() {
        //         if data.file_map_block_nr != 0 {
        //             // reset block-nr if lost.
        //             if let Some(block_type) = db.try_block_type(data.file_map_block_nr) {
        //                 match block_type {
        //                     WordBlockType::NotAllocated | WordBlockType::Free => {
        //                         println!("lost filemap {} -> {}", word, data.file_map_block_nr);
        //                         data.file_map_block_nr = 0;
        //                         data.file_map_idx = 0;
        //                     }
        //                     WordBlockType::WordMapHead => {
        //                         // ok
        //                     }
        //                     _ => {
        //                         return Err(
        //                             blockfile2::Error::err(FBErrorKind::RecoverFailed).into()
        //                         )
        //                     }
        //                 }
        //             } else {
        //                 data.file_map_block_nr = 0;
        //                 data.file_map_idx = 0;
        //             }
        //         }
        //
        //         if data.file_map_block_nr != 0 {
        //             let mut block_nr = data.file_map_block_nr;
        //             let mut block_idx = data.file_map_idx;
        //             loop {
        //                 let block = db.get_mut(block_nr)?;
        //                 let list = block.cast_mut::<RawWordMapList>();
        //                 let map = &mut list[block_idx as usize];
        //
        //                 let mut dirty = false;
        //                 for f in &mut map.file_id {
        //                     if *f != 0 && !files.list().contains_key(f) {
        //                         println!("lost file {} -> {}", word, f);
        //                         // we can handle some gaps in the data.
        //                         *f = 0;
        //                         dirty = true;
        //                     }
        //                 }
        //
        //                 let mut next_block_nr = map.next_block_nr;
        //                 let mut next_block_idx = map.next_idx;
        //
        //                 block.set_dirty(dirty);
        //
        //                 // lost the rest?
        //                 if next_block_nr != 0
        //                     && db.try_block_type(next_block_nr) != Some(WordBlockType::WordMapTail)
        //                 {
        //                     println!("lost filemap {} -> {}", word, next_block_nr);
        //                     let block = db.get_mut(block_nr)?;
        //                     let list = block.cast_mut::<RawWordMapList>();
        //                     let map = &mut list[block_idx as usize];
        //
        //                     map.next_block_nr = 0;
        //                     map.next_idx = 0;
        //                     block.set_dirty(true);
        //
        //                     next_block_nr = 0;
        //                     next_block_idx = 0;
        //                 }
        //
        //                 block_nr = next_block_nr;
        //                 block_idx = next_block_idx;
        //
        //                 if block_nr == 0 {
        //                     break;
        //                 }
        //             }
        //         }
        //     }
        //
        //     Self::load(db)
        // }

        pub fn load(db: &mut WordFileBlocks) -> Result<WordMap, IndexError> {
            let mut max_head_nr = 0u32.into();
            let mut max_tail_nr = 0u32.into();
            for (block_nr, block_type) in db.iter_metadata() {
                match block_type {
                    WordBlockType::WordMapHead => {
                        max_head_nr = max(max_head_nr, block_nr);
                    }
                    WordBlockType::WordMapTail => {
                        max_tail_nr = max(max_tail_nr, block_nr);
                    }
                    _ => {
                        // dont need this
                    }
                }
            }

            let max_head_idx = Self::load_free_idx(db, max_head_nr)?;
            let max_tail_idx = Self::load_free_idx(db, max_tail_nr)?;

            Ok(Self {
                last_block_nr_head: max_head_nr,
                last_idx_head: max_head_idx,
                last_block_nr_tail: max_tail_nr,
                last_idx_tail: max_tail_idx,
            })
        }

        fn load_free_idx(db: &mut WordFileBlocks, block_nr: LogicalNr) -> Result<u32, IndexError> {
            let empty = RawWordMap::default();
            if block_nr > 0 {
                let block = db.get(block_nr)?;
                let last = block.cast::<RawWordMapList>();
                if let Some(empty_pos) = last.iter().position(|v| *v == empty) {
                    Ok(empty_pos as u32)
                } else {
                    Ok(RawWordMapList::LEN as u32 - 1)
                }
            } else {
                Ok(0u32)
            }
        }

        pub fn store(&mut self, _db: &mut WordFileBlocks) -> Result<(), IndexError> {
            Ok(())
        }

        fn confirm_add_head(&mut self, last_block_nr_head: LogicalNr, last_idx_head: BlkIdx) {
            self.last_block_nr_head = last_block_nr_head;
            self.last_idx_head = last_idx_head;
        }

        // Ensures we can add at least 1 new region.
        fn ensure_add_head(
            &mut self,
            db: &mut WordFileBlocks,
        ) -> Result<(LogicalNr, BlkIdx), IndexError> {
            #[allow(clippy::collapsible_else_if)]
            let v = if self.last_block_nr_head == 0 {
                let new_block_nr = db.alloc(Self::TY_LISTHEAD)?.block_nr();

                self.last_block_nr_head = new_block_nr;
                self.last_idx_head = 0;

                (self.last_block_nr_head, self.last_idx_head)
            } else {
                if self.last_idx_head + 1 >= RawWordMapList::LEN as u32 {
                    let new_block_nr = db.alloc(Self::TY_LISTHEAD)?.block_nr();

                    self.last_block_nr_head = new_block_nr;
                    self.last_idx_head = 0;

                    (self.last_block_nr_head, self.last_idx_head)
                } else {
                    (self.last_block_nr_head, self.last_idx_head + 1)
                }
            };

            Ok(v)
        }

        fn confirm_add_tail(&mut self, last_block_nr_tail: LogicalNr, last_idx_tail: BlkIdx) {
            self.last_block_nr_tail = last_block_nr_tail;
            self.last_idx_tail = last_idx_tail;
        }

        // Ensures we can add at least 1 new region.
        fn ensure_add_tail(
            &mut self,
            db: &mut WordFileBlocks,
        ) -> Result<(LogicalNr, BlkIdx), IndexError> {
            #[allow(clippy::collapsible_else_if)]
            let v = if self.last_block_nr_tail == 0 {
                let new_block_nr = db.alloc(Self::TY_LISTTAIL)?.block_nr();

                self.last_block_nr_tail = new_block_nr;
                self.last_idx_tail = 0;

                (self.last_block_nr_tail, self.last_idx_tail)
            } else {
                if self.last_idx_tail + 1 >= RawWordMapList::LEN as u32 {
                    let new_block_nr = db.alloc(Self::TY_LISTTAIL)?.block_nr();

                    self.last_block_nr_tail = new_block_nr;
                    self.last_idx_tail = 0;

                    (self.last_block_nr_tail, self.last_idx_tail)
                } else {
                    (self.last_block_nr_tail, self.last_idx_tail + 1)
                }
            };

            Ok(v)
        }

        /// Add first reference for a new word.
        pub fn add_initial(
            &mut self,
            db: &mut WordFileBlocks,
            _word: &str,
            file_id: FileId,
        ) -> Result<(LogicalNr, BlkIdx), IndexError> {
            let (new_blk_nr, new_idx) = self.ensure_add_head(db)?;

            let block = db.get_mut(new_blk_nr)?;
            block.set_dirty(true);
            dbg!(&block);
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
            blk_nr: LogicalNr,
            blk_idx: BlkIdx,
            file_id: FileId,
        ) -> Result<(), IndexError> {
            // append to given region list.
            {
                let (retire_block_nr, retire_idx) = self.ensure_add_tail(db)?;

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

                    self.confirm_add_tail(retire_block_nr, retire_idx);
                }
            }
            Ok(())
        }

        pub fn iter_files(
            db: &mut WordFileBlocks,
            block_nr: LogicalNr,
            block_idx: BlkIdx,
            first_file_id: FileId,
        ) -> IterFileId {
            IterFileId {
                db,
                first_file_id,
                map_block_nr: block_nr,
                map_idx: block_idx,
                file_idx: 0,
            }
        }
    }

    pub struct IterFileId<'a> {
        db: &'a mut WordFileBlocks,
        first_file_id: FileId,
        map_block_nr: LogicalNr,
        map_idx: BlkIdx,
        file_idx: FIdx,
    }

    impl<'a> IterFileId<'a> {
        fn is_clear(&self) -> bool {
            self.map_block_nr == 0 && self.first_file_id == 0
        }

        fn clear(&mut self) {
            self.first_file_id = 0;
            self.map_block_nr = 0.into();
            self.map_idx = 0;
            self.file_idx = 0;
        }
    }

    impl<'a> Iterator for IterFileId<'a> {
        type Item = Result<FileId, IndexError>;

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
                    Err(err) => return Some(Err(err.into())),
                };
                let map = &map_list[self.map_idx as usize];
                let file_id = map.file_id[self.file_idx as usize];

                #[allow(clippy::collapsible_else_if)]
                if file_id != 0 {
                    // next
                    self.file_idx += 1;
                    if self.file_idx >= map.file_id.len() as u32 {
                        self.map_block_nr = map.next_block_nr;
                        self.map_idx = map.next_idx;
                        self.file_idx = 0;
                    }
                    break Some(file_id);
                } else if self.file_idx + 1 < map.file_id.len() as u32 {
                    // recover can leave 0 in the middle of the list.
                    self.file_idx += 1;
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

            file_id.map(Ok)
        }
    }
}

pub mod files {
    use crate::index2::{BlkIdx, FileId, IndexError, WordBlockType, WordFileBlocks, BLOCK_SIZE};
    use blockfile2::LogicalNr;
    use std::collections::BTreeMap;
    use std::fmt::Debug;

    #[derive(Debug)]
    pub struct FileList {
        last_file_id: u32,
        last_block_nr: LogicalNr,
        last_block_idx: u32,
        list: BTreeMap<FileId, FileData>,
    }

    #[derive(Debug)]
    pub struct FileData {
        pub name: String,
        pub block_nr: LogicalNr,
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

        pub(crate) fn recover(db: &mut WordFileBlocks) -> Result<FileList, IndexError> {
            Self::load(db)
        }

        pub(crate) fn load(db: &mut WordFileBlocks) -> Result<FileList, IndexError> {
            let mut list = BTreeMap::new();

            let mut last_file_id = 0u32;
            let mut last_block_nr = 0u32.into();
            let mut last_block_idx = 0u32;

            let blocks: Vec<_> = db
                .iter_metadata()
                .filter(|v| v.1 == Self::TY)
                .map(|v| v.0)
                .collect();

            for block_nr in blocks {
                let block = db.get(block_nr)?;
                let mut idx = 0usize;

                'f: loop {
                    if idx + 4 >= block.data.len() {
                        break 'f;
                    }
                    let mut file_id = [0u8; 4];
                    file_id.copy_from_slice(&block.data[idx..idx + 4]);
                    let file_id = FileId::from_ne_bytes(file_id);
                    if file_id == 0 {
                        last_block_nr = block_nr;
                        last_block_idx = idx as u32;
                        break 'f;
                    }
                    last_file_id = file_id;
                    let name_len = block.data[idx + 4] as usize;
                    let name = &block.data[idx + 5..idx + 5 + name_len];

                    list.insert(
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

            Ok(Self {
                last_file_id,
                last_block_nr,
                last_block_idx,
                list,
            })
        }

        pub(crate) fn store(&mut self, db: &mut WordFileBlocks) -> Result<(), IndexError> {
            // assume append only
            for (file_id, file_data) in self.list.iter_mut() {
                if file_data.block_nr == 0 {
                    if self.last_block_nr == 0 {
                        self.last_block_nr = db.alloc(Self::TY)?.block_nr();
                        self.last_block_idx = 0;
                    }

                    assert!(file_data.name.len() < 256);

                    let file_name = file_data.name.as_bytes();

                    let mut buf: Vec<u8> = Vec::new();
                    buf.extend(file_id.to_ne_bytes());
                    buf.extend((file_name.len() as u8).to_ne_bytes());
                    buf.extend(file_name);

                    let mut block = db.get_mut(self.last_block_nr)?;
                    let mut idx = self.last_block_idx as usize;
                    if idx + buf.len() > BLOCK_SIZE {
                        block = db.alloc(Self::TY)?;
                        self.last_block_nr = block.block_nr();
                        self.last_block_idx = 0;
                        idx = 0;
                    }
                    block.set_dirty(true);
                    block.set_discard(true);

                    let raw_buf = block.data.get_mut(idx..idx + buf.len()).expect("buffer");
                    raw_buf.copy_from_slice(buf.as_slice());

                    file_data.block_nr = self.last_block_nr;
                    file_data.block_idx = self.last_block_idx;

                    self.last_block_idx += buf.len() as u32;
                } else {
                    // no updates
                }
            }

            Ok(())
        }

        pub fn add(&mut self, name: String) -> FileId {
            self.last_file_id += 1;
            self.list.insert(
                self.last_file_id,
                FileData {
                    name,
                    block_nr: 0.into(),
                    block_idx: 0,
                },
            );
            self.last_file_id
        }

        pub fn list(&self) -> &BTreeMap<FileId, FileData> {
            &self.list
        }

        pub fn len(&self) -> usize {
            self.list.len()
        }

        pub fn is_empty(&self) -> bool {
            self.list.is_empty()
        }

        pub fn last_file_id(&self) -> FileId {
            self.last_file_id
        }
    }
}

pub mod words {
    use crate::index2::{
        byte_to_str, byte_to_string, copy_fix, BlkIdx, FileId, IndexError, WordBlockType,
        WordFileBlocks, WordId, BLOCK_SIZE,
    };
    use blockfile2::{Length, LogicalNr};
    use std::collections::BTreeMap;
    use std::fmt::{Debug, Formatter};
    use std::mem::size_of;
    use std::str::from_utf8;

    #[derive(Debug)]
    pub struct WordList {
        last_block_nr: LogicalNr,
        last_block_idx: u32,
        last_word_id: u32,
        list: BTreeMap<String, WordData>,
    }

    #[derive(Debug, Clone, Copy)]
    pub struct WordData {
        pub id: WordId,
        pub block_nr: LogicalNr,
        pub block_idx: BlkIdx,
        pub file_map_block_nr: LogicalNr,
        pub file_map_idx: BlkIdx,
        pub first_file_id: FileId,
    }

    pub type RawWordList = [RawWord; BLOCK_SIZE / size_of::<RawWord>()];

    #[derive(Clone, Copy, PartialEq)]
    #[repr(C)]
    pub struct RawWord {
        pub word: [u8; 20],
        pub id: WordId,
        pub file_map_block_nr: LogicalNr,
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
                file_map_block_nr: 0.into(),
                file_map_idx_or_file_id: 0,
            }
        }
    }

    impl WordList {
        pub const TY: WordBlockType = WordBlockType::WordList;

        pub(crate) fn recover(
            db: &mut WordFileBlocks,
            max_file_id: u32,
        ) -> Result<WordList, IndexError> {
            let mut words = Self::load(db)?;

            for data in words.list.values_mut() {
                if data.first_file_id > max_file_id {
                    data.first_file_id = 0;
                }
            }

            Ok(words)
        }

        pub(crate) fn load(db: &mut WordFileBlocks) -> Result<WordList, IndexError> {
            let mut list = BTreeMap::new();

            let mut last_block_nr = 0u32.into();
            let mut last_block_idx = 0u32;
            let mut last_word_id = 0u32;

            let blocks: Vec<_> = db
                .iter_metadata()
                .filter(|v| v.1 == Self::TY)
                .map(|v| v.0)
                .collect();
            let empty = RawWord::default();
            for block_nr in blocks {
                let block = db.get(block_nr)?;
                let raw = block.cast::<RawWordList>();
                for (i, r) in raw.iter().enumerate() {
                    if r.word != empty.word {
                        let word = match byte_to_str(&r.word) {
                            Ok(v) => v.to_string(),
                            Err(IndexError::Utf8Error(_b)) => {
                                // we cut the words to 20 bytes, so there will be some
                                // errors when reading the data.
                                byte_to_string(&r.word)
                            }
                            Err(e) => return Err(e),
                        };

                        // remember
                        last_word_id = r.id;
                        last_block_nr = block_nr;
                        last_block_idx = i as u32 + 1;

                        // block_nr == 0 means we have only one file-id and it is stored
                        // as file_map_idx.
                        if r.file_map_block_nr == 0 {
                            list.insert(
                                word,
                                WordData {
                                    id: r.id,
                                    block_nr,
                                    block_idx: i as u32,
                                    file_map_block_nr: 0.into(),
                                    file_map_idx: 0,
                                    first_file_id: r.file_map_idx_or_file_id,
                                },
                            );
                        } else {
                            list.insert(
                                word,
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
            }

            // Check overflow
            if last_block_idx >= RawWordList::LEN as u32 {
                last_block_nr = db.alloc(Self::TY)?.block_nr();
                last_block_idx = 0;
            }

            Ok(Self {
                last_block_nr,
                last_block_idx,
                last_word_id,
                list,
            })
        }

        pub(crate) fn store(&mut self, db: &mut WordFileBlocks) -> Result<(), IndexError> {
            // assume append only
            for (word, word_data) in self.list.iter_mut() {
                let w = if word_data.first_file_id != 0 {
                    RawWord {
                        word: copy_fix::<20>(word.as_bytes()),
                        id: word_data.id,
                        file_map_block_nr: 0.into(),
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
                    }
                } else {
                    if self.last_block_nr == 0 {
                        self.last_block_nr = db.alloc(Self::TY)?.block_nr();
                        self.last_block_idx = 0;
                    }

                    let block = db.get_mut(self.last_block_nr)?;
                    block.set_dirty(true);
                    // block.discard();
                    let word_list = block.cast_mut::<RawWordList>();
                    word_list[self.last_block_idx as usize] = w; //todo: XXS!
                    word_data.block_nr = self.last_block_nr;
                    word_data.block_idx = self.last_block_idx;

                    if self.last_block_idx + 1 == RawWordList::LEN as u32 {
                        self.last_block_nr = db.alloc(Self::TY)?.block_nr();
                        self.last_block_idx = 0;
                    } else {
                        self.last_block_idx += 1;
                    }
                }
            }

            Ok(())
        }

        /// Iterate words.
        pub fn iter_words(&mut self) -> impl Iterator<Item = (&String, &WordData)> {
            self.list.iter()
        }

        pub fn len(&self) -> usize {
            self.list.len()
        }

        pub fn is_empty(&self) -> bool {
            self.list.is_empty()
        }

        pub fn list(&self) -> &BTreeMap<String, WordData> {
            &self.list
        }

        pub fn list_mut(&mut self) -> &mut BTreeMap<String, WordData> {
            &mut self.list
        }

        pub fn get_mut(&mut self, word: &str) -> Option<&mut WordData> {
            self.list.get_mut(word)
        }

        pub fn insert<S: AsRef<str>>(&mut self, word: S, file_id: FileId) {
            self.last_word_id += 1;
            self.list.insert(
                word.as_ref().into(),
                WordData {
                    id: self.last_word_id,
                    block_nr: 0.into(),
                    block_idx: 0,
                    file_map_block_nr: 0.into(),
                    file_map_idx: 0,
                    first_file_id: file_id,
                },
            );
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

fn byte_to_str<const N: usize>(src: &[u8; N]) -> Result<&str, IndexError> {
    let Ok(word) = from_utf8(src.as_ref()) else {
        return Err(IndexError::Utf8Error(Vec::from(src.as_ref())));
    };
    let word = word.trim_end_matches('\0');
    Ok(word)
}

fn byte_to_string<const N: usize>(src: &[u8; N]) -> String {
    let word = String::from_utf8_lossy(src.as_ref()).to_string();
    let word = word.trim_end_matches('\0');
    word.to_string()
}
