#![allow(dead_code)]

mod files;
mod ids;
mod word_map;
mod words;

pub mod tmp_index;

use ids::{BlkIdx, FIdx, FileId, WordId};

use crate::index2::files::{FileData, FileList};
use crate::index2::tmp_index::TmpWords;
use crate::index2::word_map::{RawWordMapList, WordMap};
use crate::index2::words::{RawWordList, WordData, WordList};
use blockfile2::{BlockType, FileBlocks, UserBlockType};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Display, Formatter};
use std::fs;
use std::mem::align_of;
use std::path::Path;
use std::str::from_utf8;
use wildmatch::WildMatch;

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

impl TryFrom<u32> for WordBlockType {
    type Error = u32;
    fn try_from(value: u32) -> Result<Self, Self::Error> {
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
            WordBlockType::WordList => BlockType::User1,
            WordBlockType::FileList => BlockType::User2,
            WordBlockType::WordMapHead => BlockType::User3,
            WordBlockType::WordMapTail => BlockType::User4,
        }
    }

    fn user_type(block_type: BlockType) -> Option<Self> {
        match block_type {
            BlockType::User1 => Some(Self::WordList),
            BlockType::User2 => Some(Self::FileList),
            BlockType::User3 => Some(Self::WordMapHead),
            BlockType::User4 => Some(Self::WordMapTail),
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
                    match WordBlockType::try_from(i as u32) {
                        Ok(v) => v.to_string(),
                        Err(e) => match BlockType::try_from(e) {
                            Ok(v) => v.to_string(),
                            Err(e) => e.to_string(),
                        },
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

                    data.first_file_id = FileId(0);
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
