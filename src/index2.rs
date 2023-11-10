#![allow(dead_code)]

pub mod files;
pub mod ids;
pub mod tmp_index;
pub mod word_map;
pub mod words;

use crate::index2::files::{FileData, FileList};
use crate::index2::tmp_index::TmpWords;
use crate::index2::word_map::{RawBags, RawWordMap, WordMap, BAG_LEN};
use crate::index2::words::{RawWord, WordData, WordList};
use blockfile2::{BlockType, FileBlocks, UserBlockType};
use ids::{BlkIdx, FIdx, FileId, WordId};
use std::backtrace::Backtrace;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Display, Formatter};
use std::mem::align_of;
use std::path::Path;
use std::str::from_utf8;
use std::time::Instant;
use std::{fs, io, string};
use wildmatch::WildMatch;

pub struct IndexError {
    pub kind: IndexKind,
    pub backtrace: Backtrace,
}

#[derive(Debug)]
pub enum IndexKind {
    BlockFile(blockfile2::Error),
    Utf8Error(Vec<u8>),
    FromUtf8Error(string::FromUtf8Error),
    IOError(io::Error),
}

impl Display for IndexKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            IndexKind::BlockFile(e) => write!(f, "BlockFile {:?}", e),
            IndexKind::Utf8Error(v) => write!(f, "Utf8Error {:?}", v),
            IndexKind::IOError(v) => write!(f, "IOError {:?}", v),
            IndexKind::FromUtf8Error(v) => write!(f, "FromUtf8Error {:?}", v),
        }
    }
}

impl IndexError {
    pub fn err(kind: IndexKind) -> Self {
        Self {
            kind,
            backtrace: Backtrace::capture(),
        }
    }
}

impl Debug for IndexError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "{:#}", self.kind)?;
        writeln!(f, "{:#}", self.backtrace)?;
        Ok(())
    }
}

impl Display for IndexError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "{:#}", self.kind)?;
        writeln!(f, "{:#}", self.backtrace)?;
        Ok(())
    }
}

impl From<blockfile2::Error> for IndexError {
    fn from(value: blockfile2::Error) -> Self {
        IndexError::err(IndexKind::BlockFile(value))
    }
}

impl From<io::Error> for IndexError {
    fn from(value: io::Error) -> Self {
        IndexError::err(IndexKind::IOError(value))
    }
}

impl From<string::FromUtf8Error> for IndexError {
    fn from(value: string::FromUtf8Error) -> Self {
        IndexError::err(IndexKind::FromUtf8Error(value))
    }
}

impl std::error::Error for IndexError {}

const BLOCK_SIZE: usize = 4096;

pub struct Words {
    pub db: WordFileBlocks,
    words: WordList,
    word_count: usize,
    bag_stats: [usize; BAG_LEN],
    files: FileList,
    wordmap: WordMap,
    auto_save: u32,
    save_time: Instant,
}

pub type WordFileBlocks = FileBlocks<WordBlockType>;

#[derive(Clone, Copy, PartialEq)]
pub enum WordBlockType {
    WordList = BlockType::User1 as isize,
    FileList = BlockType::User2 as isize,
    WordMapHead = BlockType::User3 as isize,
    WordMapTail = BlockType::User4 as isize,
    WordMapBags = BlockType::User5 as isize,
}

impl TryFrom<u32> for WordBlockType {
    type Error = u32;
    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            16 => Ok(WordBlockType::WordList),
            17 => Ok(WordBlockType::FileList),
            18 => Ok(WordBlockType::WordMapHead),
            19 => Ok(WordBlockType::WordMapTail),
            20 => Ok(WordBlockType::WordMapBags),
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
            WordBlockType::WordMapBags => "WBG",
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
            WordBlockType::WordMapBags => BlockType::User5,
        }
    }

    fn user_type(block_type: BlockType) -> Option<Self> {
        match block_type {
            BlockType::User1 => Some(Self::WordList),
            BlockType::User2 => Some(Self::FileList),
            BlockType::User3 => Some(Self::WordMapHead),
            BlockType::User4 => Some(Self::WordMapTail),
            BlockType::User5 => Some(Self::WordMapBags),
            _ => None,
        }
    }

    fn align(self) -> usize {
        match self {
            WordBlockType::WordList => align_of::<[RawWord; 1]>(),
            WordBlockType::FileList => align_of::<[u8; 1]>(),
            WordBlockType::WordMapHead => align_of::<[RawWordMap; 1]>(),
            WordBlockType::WordMapTail => align_of::<[RawWordMap; 1]>(),
            WordBlockType::WordMapBags => align_of::<RawBags>(),
        }
    }

    fn is_stream(self) -> bool {
        match self {
            WordBlockType::FileList => true,
            _ => false,
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
                .field("word_count", &self.word_count)
                .field("bag_stats", &RefSlice(&self.bag_stats, 0))
                .field("db", &self.db)
                .finish()?;
        } else if f.width().unwrap_or(0) >= 1 {
            f.debug_struct("Words")
                .field("words", &self.words)
                .field("files", &self.files)
                .field("wordmap", &self.wordmap)
                .field("word_count", &self.word_count)
                .field("bag_stats", &RefSlice(&self.bag_stats, 0))
                .field("db", &self.db)
                .finish()?;
        }

        struct RefSlice<'a, T>(&'a [T], usize);
        impl<'a, T> Debug for RefSlice<'a, T>
        where
            T: Debug,
        {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                for r in 0..(self.0.len() + 16) / 16 {
                    writeln!(f)?;
                    write!(f, "{:9}: ", self.1 + r * 16)?;
                    for c in 0..16 {
                        let i = r * 16 + c;

                        if i < self.0.len() {
                            write!(f, "{:4?} ", self.0[i])?;
                        }
                    }
                }
                Ok(())
            }
        }

        writeln!(f)?;
        for block in self.db.iter_blocks() {
            match WordBlockType::user_type(block.block_type()) {
                Some(WordBlockType::WordList) => {
                    let data = unsafe { block.cast_array::<RawWord>() };
                    writeln!(f, "WordList {}", block.block_nr())?;
                    if f.width().unwrap_or(0) >= 1 {
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
                }
                Some(WordBlockType::FileList) => {
                    writeln!(f, "FileList {}", block.block_nr())?;
                    if f.width().unwrap_or(0) >= 1 {
                        writeln!(f, "{:?}", block)?;
                    }
                }
                Some(WordBlockType::WordMapHead) => {
                    let data = unsafe { block.cast_array::<RawWordMap>() };
                    writeln!(f, "WordMapHead {}", block.block_nr())?;
                    if f.width().unwrap_or(0) >= 1 {
                        for d in data.iter() {
                            writeln!(f, "{:?} -> {} {}", d.file_id, d.next_block_nr, d.next_idx)?;
                        }
                    }
                }
                Some(WordBlockType::WordMapTail) => {
                    let data = unsafe { block.cast_array::<RawWordMap>() };
                    writeln!(f, "WordMapTail {}", block.block_nr())?;
                    if f.width().unwrap_or(0) >= 1 {
                        for d in data.iter() {
                            writeln!(f, "{:?} -> {} {}", d.file_id, d.next_block_nr, d.next_idx)?;
                        }
                    }
                }
                Some(WordBlockType::WordMapBags) => {
                    let data = unsafe { block.cast::<RawBags>() };
                    writeln!(f, "WordMapBags {}", block.block_nr())?;
                    if f.width().unwrap_or(0) >= 1 {
                        for i in 0..BAG_LEN {
                            writeln!(
                                f,
                                "H{}:{} T{}:{}",
                                data.head_nr[i],
                                data.head_idx[i],
                                data.tail_nr[i],
                                data.tail_idx[i]
                            )?;
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

        Self::cleanup(&mut db)?;

        Ok(Self {
            db,
            words,
            word_count: 0,
            bag_stats: [0usize; BAG_LEN],
            files,
            wordmap,
            auto_save: 0,
            save_time: Instant::now(),
        })
    }

    pub fn write(&mut self) -> Result<(), IndexError> {
        self.words.store(&mut self.db)?;
        self.files.store(&mut self.db)?;
        self.wordmap.store(&mut self.db)?;

        self.write_stats();

        self.db.store()?;

        Self::cleanup(&mut self.db)?;
        Ok(())
    }

    fn cleanup(db: &mut WordFileBlocks) -> Result<(), IndexError> {
        // let generation = self.db.generation();

        // retain some datablocks in memory.
        db.retain(|_k, v| match WordBlockType::user_type(v.block_type()) {
            Some(WordBlockType::WordList) => false,
            Some(WordBlockType::FileList) => false,
            Some(WordBlockType::WordMapHead) => false,
            Some(WordBlockType::WordMapTail) => false,
            Some(WordBlockType::WordMapBags) => true,
            None => false, // doesn't matter
        });
        Ok(())
    }

    pub fn compact_blocks(&mut self) {
        // todo: self.db.compact_to()
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

    pub fn find_file(&self, txt: &str) -> Vec<String> {
        let find = WildMatch::new(txt);
        self.files
            .list()
            .values()
            .filter(|v| find.matches(v.name.as_str()))
            .map(|v| &v.name)
            .cloned()
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
        )
    }

    /// Total word count.
    pub fn add_word_count(&mut self, count: usize) {
        self.word_count += count;
    }

    /// Add a word and a file reference.
    /// It is not checked, if the reference was already inserted.
    /// Duplicates are acceptable.
    pub fn add_word<S: AsRef<str>>(
        &mut self,
        word: S,
        count: usize,
        file_id: FileId,
    ) -> Result<(), IndexError> {
        if let Some(data) = self.words.get_mut(word.as_ref()) {
            data.count += count;

            let bag = if self.word_count == 0 {
                0
            } else {
                // a single word should hardly have more than 5% of total word count.
                let v = (data.count * 256 * 20) / self.word_count;
                clamp(0, 255, v)
            };
            self.bag_stats[bag] += 1;

            // add second file-id. (and any further).
            self.wordmap.add(
                &mut self.db,
                word.as_ref(),
                bag,
                data.file_map_block_nr,
                data.file_map_idx,
                file_id,
            )?;
        } else {
            let bag = if self.word_count == 0 {
                0
            } else {
                // a single word should hardly have more than 5% of total word count.
                let v = (count * 256 * 20) / self.word_count;
                clamp(0, 255, v)
            };
            self.bag_stats[bag] += 1;

            // Initial references get a special block.
            let (file_map_block_nr, file_map_idx) =
                self.wordmap
                    .add_initial(&mut self.db, bag, word.as_ref(), file_id)?;

            self.words
                .insert(word, count, file_map_block_nr, file_map_idx);
        };
        Ok(())
    }

    /// Append a temp buffer for a file.
    pub fn append(&mut self, other: TmpWords) -> Result<(), IndexError> {
        let f_idx = self.add_file(other.file);
        self.add_word_count(other.count);
        for (a_txt, a_n) in other.words.iter() {
            self.add_word(a_txt, *a_n, f_idx)?;
        }
        Ok(())
    }

    /// Find words.
    pub fn find(&mut self, terms: &[String]) -> Result<Vec<String>, IndexError> {
        let mut collect = BTreeSet::<FileId>::new();
        let mut first = true;

        let terms: Vec<_> = terms.iter().map(|v| WildMatch::new(v)).collect();

        // find the words and the files where they are contained.
        // each consecutive search-term *reduces* the list of viable files.
        for matcher in terms {
            let words: Vec<_> = self
                .iter_words()
                .filter(|(k, _)| matcher.matches(k))
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

        // map the found file-id to the file-name.
        let names = collect.iter().flat_map(|v| self.file(*v)).collect();

        Ok(names)
    }

    pub fn set_save_time(&mut self) {
        self.save_time = Instant::now();
    }

    pub fn save_time(&self) -> Instant {
        self.save_time
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
        // trim incomplete utf8 sequence at end.
        for i in (0..LEN).rev() {
            if dst[i] >= 192 {
                // clear start byte and stop
                dst[i] = 0;
                break;
            } else if dst[i] >= 128 {
                // clear followup byte
                dst[i] = 0;
            } else {
                break;
            }
        }
    }
    dst
}

fn byte_to_str<const N: usize>(src: &[u8; N]) -> Result<&str, IndexError> {
    let Ok(word) = from_utf8(src.as_ref()) else {
        return Err(IndexError::err(IndexKind::Utf8Error(Vec::from(
            src.as_ref(),
        ))));
    };
    let word = word.trim_end_matches('\0');
    Ok(word)
}

fn byte_to_string<const N: usize>(src: &[u8; N]) -> String {
    let word = String::from_utf8_lossy(src.as_ref()).to_string();
    let word = word.trim_end_matches('\0');
    word.to_string()
}

fn clamp(min: usize, max: usize, val: usize) -> usize {
    usize::max(min, usize::min(val, max))
}
