use crate::index2::{BlkIdx, FIdx, FileId, IndexError, WordBlockType, WordFileBlocks, BLOCK_SIZE};
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
        let mut max_head_nr = LogicalNr(0u32);
        let mut max_tail_nr = LogicalNr(0u32);
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

    fn load_free_idx(db: &mut WordFileBlocks, block_nr: LogicalNr) -> Result<BlkIdx, IndexError> {
        let empty = RawWordMap::default();
        if block_nr > 0 {
            let block = db.get(block_nr)?;
            let last = block.cast::<RawWordMapList>();
            if let Some(empty_pos) = last.iter().position(|v| *v == empty) {
                Ok(BlkIdx(empty_pos as u32))
            } else {
                Ok(BlkIdx(RawWordMapList::LEN as u32 - 1))
            }
        } else {
            Ok(BlkIdx(0u32))
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
            self.last_idx_head = BlkIdx(0);

            (self.last_block_nr_head, self.last_idx_head)
        } else {
            if self.last_idx_head + 1 >= RawWordMapList::LEN as u32 {
                let new_block_nr = db.alloc(Self::TY_LISTHEAD)?.block_nr();

                self.last_block_nr_head = new_block_nr;
                self.last_idx_head = BlkIdx(0);

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
            self.last_idx_tail = BlkIdx(0);

            (self.last_block_nr_tail, self.last_idx_tail)
        } else {
            if self.last_idx_tail + 1 >= RawWordMapList::LEN as u32 {
                let new_block_nr = db.alloc(Self::TY_LISTTAIL)?.block_nr();

                self.last_block_nr_tail = new_block_nr;
                self.last_idx_tail = BlkIdx(0);

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

        let word_map_list = block.cast_mut::<RawWordMapList>();
        let word_map = &mut word_map_list[new_idx.as_usize()];

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
            let word_map = &mut word_map_list[blk_idx.as_usize()];

            if let Some(insert_pos) = word_map.file_id.iter().position(|v| *v == 0) {
                word_map.file_id[insert_pos] = file_id;
            } else {
                // move out of current
                let retire_file_id = word_map.file_id;
                let retire_next_block_nr = word_map.next_block_nr;
                let retire_next_idx = word_map.next_idx;

                // re-init and write
                word_map.file_id = [FileId(0u32); FILE_ID_LEN];
                word_map.next_block_nr = retire_block_nr;
                word_map.next_idx = retire_idx;
                word_map.file_id[0] = file_id;

                // retire
                let retire_block = db.get_mut(self.last_block_nr_tail)?;
                retire_block.set_dirty(true);
                let retire_map_list = retire_block.cast_mut::<RawWordMapList>();
                let retire_map = &mut retire_map_list[retire_idx.as_usize()];

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
    ) -> IterFileId {
        IterFileId {
            db,
            map_block_nr: block_nr,
            map_idx: block_idx,
            file_idx: FIdx(0),
        }
    }
}

pub struct IterFileId<'a> {
    db: &'a mut WordFileBlocks,
    map_block_nr: LogicalNr,
    map_idx: BlkIdx,
    file_idx: FIdx,
}

impl<'a> IterFileId<'a> {
    fn is_clear(&self) -> bool {
        self.map_block_nr == 0
    }

    fn clear(&mut self) {
        self.map_block_nr = LogicalNr(0);
        self.map_idx = BlkIdx(0);
        self.file_idx = FIdx(0);
    }
}

impl<'a> Iterator for IterFileId<'a> {
    type Item = Result<FileId, IndexError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.is_clear() {
            return None;
        }

        let file_id = loop {
            let map_list = match self.db.get(self.map_block_nr) {
                Ok(block) => block.cast::<RawWordMapList>(),
                Err(err) => return Some(Err(err.into())),
            };
            let map = &map_list[self.map_idx.as_usize()];
            let file_id = map.file_id[self.file_idx.as_usize()];

            #[allow(clippy::collapsible_else_if)]
            if file_id != 0 {
                // next
                self.file_idx += 1;
                if self.file_idx >= map.file_id.len() as u32 {
                    self.map_block_nr = map.next_block_nr;
                    self.map_idx = map.next_idx;
                    self.file_idx = FIdx(0);
                }
                break Some(file_id);
            } else if self.file_idx + 1 < map.file_id.len() as u32 {
                // recover can leave 0 in the middle of the list.
                self.file_idx += 1;
            } else {
                if map.next_block_nr != 0 {
                    self.map_block_nr = map.next_block_nr;
                    self.map_idx = map.next_idx;
                    self.file_idx = FIdx(0);
                } else {
                    break None;
                }
            }
        };

        file_id.map(Ok)
    }
}
