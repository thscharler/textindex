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
    last_block_idx: BlkIdx,
    last_word_id: WordId,
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
    //?
    pub id: WordId,
    pub file_map_block_nr: LogicalNr,
    pub file_map_idx_or_file_id: u32,
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
            id: WordId(0),
            file_map_block_nr: LogicalNr(0),
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
                data.first_file_id = FileId(0);
            }
        }

        Ok(words)
    }

    pub(crate) fn load(db: &mut WordFileBlocks) -> Result<WordList, IndexError> {
        let mut list = BTreeMap::new();

        let mut last_block_nr = LogicalNr(0u32);
        let mut last_block_idx = BlkIdx(0u32);
        let mut last_word_id = WordId(0u32);

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
                    last_block_idx = BlkIdx(i as u32 + 1);

                    // block_nr == 0 means we have only one file-id and it is stored
                    // as file_map_idx.
                    if r.file_map_block_nr == 0 {
                        list.insert(
                            word,
                            WordData {
                                id: r.id,
                                block_nr,
                                block_idx: BlkIdx(i as u32),
                                file_map_block_nr: LogicalNr(0),
                                file_map_idx: BlkIdx(0),
                                first_file_id: FileId(r.file_map_idx_or_file_id),
                            },
                        );
                    } else {
                        list.insert(
                            word,
                            WordData {
                                id: r.id,
                                block_nr,
                                block_idx: BlkIdx(i as u32),
                                file_map_block_nr: r.file_map_block_nr,
                                file_map_idx: BlkIdx(r.file_map_idx_or_file_id),
                                first_file_id: FileId(0),
                            },
                        );
                    }
                }
            }
        }

        // Check overflow
        if last_block_idx >= RawWordList::LEN as u32 {
            last_block_nr = db.alloc(Self::TY)?.block_nr();
            last_block_idx = BlkIdx(0);
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
                    file_map_block_nr: LogicalNr(0),
                    file_map_idx_or_file_id: word_data.first_file_id.0,
                }
            } else {
                RawWord {
                    word: copy_fix::<20>(word.as_bytes()),
                    id: word_data.id,
                    file_map_block_nr: word_data.file_map_block_nr,
                    file_map_idx_or_file_id: word_data.file_map_idx.0,
                }
            };

            if word_data.block_nr != 0 {
                let block = db.get_mut(word_data.block_nr)?;
                let word_list = block.cast_mut::<RawWordList>();

                if word_list[word_data.block_idx.as_usize()] != w {
                    word_list[word_data.block_idx.as_usize()] = w;
                    block.set_dirty(true);
                }
            } else {
                if self.last_block_nr == 0 {
                    self.last_block_nr = db.alloc(Self::TY)?.block_nr();
                    self.last_block_idx = BlkIdx(0);
                }

                let block = db.get_mut(self.last_block_nr)?;
                block.set_dirty(true);
                // block.discard();
                let word_list = block.cast_mut::<RawWordList>();
                word_list[self.last_block_idx.as_usize()] = w; //todo: XXS!
                word_data.block_nr = self.last_block_nr;
                word_data.block_idx = self.last_block_idx;

                if self.last_block_idx + 1 == RawWordList::LEN as u32 {
                    self.last_block_nr = db.alloc(Self::TY)?.block_nr();
                    self.last_block_idx = BlkIdx(0);
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
                block_nr: LogicalNr(0),
                block_idx: BlkIdx(0),
                file_map_block_nr: LogicalNr(0),
                file_map_idx: BlkIdx(0),
                first_file_id: file_id,
            },
        );
    }
}
