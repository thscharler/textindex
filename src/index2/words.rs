use crate::index2::{
    byte_to_str, copy_fix, BlkIdx, IndexError, WordBlockType, WordFileBlocks, WordId,
};
use blockfile2::{Block, LogicalNr};
use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::io::Write;
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
    pub count: usize,
    pub block_nr: LogicalNr,
    pub block_idx: BlkIdx,
    pub file_map_block_nr: LogicalNr,
    pub file_map_idx: BlkIdx,
}

#[derive(Clone, Copy, PartialEq)]
#[repr(C)]
pub struct RawWord {
    pub word: [u8; 20],
    pub id: WordId,
    pub file_map_block_nr: LogicalNr,
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
            id: WordId(0),
            file_map_block_nr: LogicalNr(0),
            file_map_idx: BlkIdx(0),
        }
    }
}

impl WordList {
    pub const TY: WordBlockType = WordBlockType::WordList;

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
            let raw = block.cast_array::<RawWord>();
            for (i, r) in raw.iter().enumerate() {
                if r.word != empty.word {
                    let word = byte_to_str(&r.word)?.to_string();

                    // remember
                    last_word_id = r.id;
                    last_block_nr = block_nr;
                    last_block_idx = BlkIdx(i as u32 + 1);

                    list.insert(
                        word,
                        WordData {
                            id: r.id,
                            count: 0,
                            block_nr,
                            block_idx: BlkIdx(i as u32),
                            file_map_block_nr: r.file_map_block_nr,
                            file_map_idx: r.file_map_idx,
                        },
                    );
                }
            }
        }

        // Check overflow
        if last_block_nr > 0 {
            if last_block_idx >= Block::len_array::<RawWord>(db.block_size()) as u32 {
                last_block_nr = db.alloc(Self::TY)?.block_nr();
                last_block_idx = BlkIdx(0);
            }
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
            let w = RawWord {
                word: copy_fix::<20>(word.as_bytes()),
                id: word_data.id,
                file_map_block_nr: word_data.file_map_block_nr,
                file_map_idx: word_data.file_map_idx,
            };

            if word_data.block_nr != 0 {
                let block = db.get_mut(word_data.block_nr)?;
                let word_list = block.cast_array_mut::<RawWord>();

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

                let word_list = block.cast_array_mut::<RawWord>();
                word_list[self.last_block_idx.as_usize()] = w; //todo: XXS!
                word_data.block_nr = self.last_block_nr;
                word_data.block_idx = self.last_block_idx;

                if self.last_block_idx + 1 == Block::len_array::<RawWord>(db.block_size()) as u32 {
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

    pub fn insert<S: AsRef<str>>(
        &mut self,
        word: S,
        count: usize,
        file_map_block_nr: LogicalNr,
        file_map_idx: BlkIdx,
    ) {
        self.last_word_id += 1;
        self.list.insert(
            word.as_ref().into(),
            WordData {
                id: self.last_word_id,
                count,
                block_nr: LogicalNr(0),
                block_idx: BlkIdx(0),
                file_map_block_nr,
                file_map_idx,
            },
        );
    }
}
