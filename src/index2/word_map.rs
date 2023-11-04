use crate::index2::{BlkIdx, FIdx, FileId, IndexError, WordBlockType, WordFileBlocks};
use blockfile2::{Block, LogicalNr};
use std::fmt::{Debug, Formatter};

pub struct WordMap {
    pub bag_nr: LogicalNr,
    pub last_head_nr: [LogicalNr; BAG_LEN],
    pub last_head_idx: [BlkIdx; BAG_LEN],
    pub last_tail_nr: [LogicalNr; BAG_LEN],
    pub last_tail_idx: [BlkIdx; BAG_LEN],
}

pub const FILE_ID_LEN: usize = 6;

#[derive(Clone, Copy, PartialEq, Default)]
#[repr(C)]
pub struct RawWordMap {
    pub file_id: [FileId; FILE_ID_LEN],
    pub next_block_nr: LogicalNr,
    pub next_idx: BlkIdx,
}

pub const BAG_LEN: usize = 256;

#[derive(Clone, Copy, PartialEq)]
#[repr(C)]
pub struct RawBags {
    pub head_nr: [LogicalNr; BAG_LEN],
    pub head_idx: [BlkIdx; BAG_LEN],
    pub tail_nr: [LogicalNr; BAG_LEN],
    pub tail_idx: [BlkIdx; BAG_LEN],
}

impl Default for RawBags {
    fn default() -> Self {
        RawBags {
            head_nr: [LogicalNr(0); BAG_LEN],
            head_idx: [BlkIdx(0); BAG_LEN],
            tail_nr: [LogicalNr(0); BAG_LEN],
            tail_idx: [BlkIdx(0); BAG_LEN],
        }
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
    pub const TY_BAGS: WordBlockType = WordBlockType::WordMapBags;
    pub const TY_LISTHEAD: WordBlockType = WordBlockType::WordMapHead;
    pub const TY_LISTTAIL: WordBlockType = WordBlockType::WordMapTail;

    pub fn load(db: &mut WordFileBlocks) -> Result<WordMap, IndexError> {
        for (block_nr, block_type) in db.iter_metadata() {
            match block_type {
                WordBlockType::WordMapBags => {
                    let block = db.get(block_nr)?;
                    let bags = block.cast::<RawBags>();

                    return Ok(Self {
                        bag_nr: block_nr,
                        last_head_nr: bags.head_nr,
                        last_head_idx: bags.head_idx,
                        last_tail_nr: bags.tail_nr,
                        last_tail_idx: bags.tail_idx,
                    });
                }
                _ => {
                    // dont need this
                }
            }
        }

        Ok(Self {
            bag_nr: LogicalNr(0),
            last_head_nr: [LogicalNr(0); BAG_LEN],
            last_head_idx: [BlkIdx(0); BAG_LEN],
            last_tail_nr: [LogicalNr(0); BAG_LEN],
            last_tail_idx: [BlkIdx(0); BAG_LEN],
        })
    }

    pub fn store(&mut self, db: &mut WordFileBlocks) -> Result<(), IndexError> {
        let block = if self.bag_nr != 0 {
            db.get_mut(self.bag_nr)?
        } else {
            let block = db.alloc(Self::TY_BAGS)?;
            self.bag_nr = block.block_nr();
            block
        };
        block.set_dirty(true);
        let bags = block.cast_mut::<RawBags>();

        bags.head_nr = self.last_head_nr;
        bags.head_idx = self.last_head_idx;
        bags.tail_nr = self.last_tail_nr;
        bags.tail_idx = self.last_tail_idx;

        Ok(())
    }

    fn confirm_add_head(&mut self, bag: usize, last_head_nr: LogicalNr, last_head_idx: BlkIdx) {
        self.last_head_nr[bag] = last_head_nr;
        self.last_head_idx[bag] = last_head_idx;
    }

    // Ensures we can add at least 1 new region.
    fn ensure_add_head(
        &mut self,
        db: &mut WordFileBlocks,
        bag: usize,
    ) -> Result<(LogicalNr, BlkIdx), IndexError> {
        #[allow(clippy::collapsible_else_if)]
        let v = if self.last_head_nr[bag] == 0 {
            let new_block_nr = db.alloc(Self::TY_LISTHEAD)?.block_nr();

            self.last_head_nr[bag] = new_block_nr;
            self.last_head_idx[bag] = BlkIdx(0);

            (self.last_head_nr[bag], self.last_head_idx[bag])
        } else {
            if self.last_head_idx[bag] + 1 >= Block::len_array::<RawWordMap>(db.block_size()) as u32
            {
                let new_block_nr = db.alloc(Self::TY_LISTHEAD)?.block_nr();

                self.last_head_nr[bag] = new_block_nr;
                self.last_head_idx[bag] = BlkIdx(0);

                (self.last_head_nr[bag], self.last_head_idx[bag])
            } else {
                (self.last_head_nr[bag], self.last_head_idx[bag] + 1)
            }
        };

        Ok(v)
    }

    fn confirm_add_tail(&mut self, bag: usize, last_tail_nr: LogicalNr, last_tail_idx: BlkIdx) {
        self.last_tail_nr[bag] = last_tail_nr;
        self.last_tail_idx[bag] = last_tail_idx;
    }

    // Ensures we can add at least 1 new region.
    fn ensure_add_tail(
        &mut self,
        db: &mut WordFileBlocks,
        bag: usize,
    ) -> Result<(LogicalNr, BlkIdx), IndexError> {
        #[allow(clippy::collapsible_else_if)]
        let v = if self.last_tail_nr[bag] == 0 {
            let new_block_nr = db.alloc(Self::TY_LISTTAIL)?.block_nr();

            self.last_tail_nr[bag] = new_block_nr;
            self.last_tail_idx[bag] = BlkIdx(0);

            (self.last_tail_nr[bag], self.last_tail_idx[bag])
        } else {
            if self.last_tail_idx[bag] + 1 >= Block::len_array::<RawWordMap>(db.block_size()) as u32
            {
                let new_block_nr = db.alloc(Self::TY_LISTTAIL)?.block_nr();

                self.last_tail_nr[bag] = new_block_nr;
                self.last_tail_idx[bag] = BlkIdx(0);

                (self.last_tail_nr[bag], self.last_tail_idx[bag])
            } else {
                (self.last_tail_nr[bag], self.last_tail_idx[bag] + 1)
            }
        };

        Ok(v)
    }

    /// Add first reference for a new word.
    pub fn add_initial(
        &mut self,
        db: &mut WordFileBlocks,
        bag: usize,
        _word: &str,
        file_id: FileId,
    ) -> Result<(LogicalNr, BlkIdx), IndexError> {
        let (new_blk_nr, new_idx) = self.ensure_add_head(db, bag)?;

        let block = db.get_mut(new_blk_nr)?;
        block.set_dirty(true);

        let word_map_list = block.cast_array_mut::<RawWordMap>();
        let word_map = &mut word_map_list[new_idx.as_usize()];

        word_map.file_id[0] = file_id;

        self.confirm_add_head(bag, new_blk_nr, new_idx);

        Ok((new_blk_nr, new_idx))
    }

    /// Add one more file reference for a word.
    pub fn add(
        &mut self,
        db: &mut WordFileBlocks,
        _word: &str,
        bag: usize,
        blk_nr: LogicalNr,
        blk_idx: BlkIdx,
        file_id: FileId,
    ) -> Result<(), IndexError> {
        // append to given region list.
        {
            let (retire_block_nr, retire_idx) = self.ensure_add_tail(db, bag)?;

            let block = db.get_mut(blk_nr)?;
            block.set_dirty(true);
            let word_map_list = block.cast_array_mut::<RawWordMap>();
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
                let retire_block = db.get_mut(self.last_tail_nr[bag])?;
                retire_block.set_dirty(true);
                let retire_map_list = retire_block.cast_array_mut::<RawWordMap>();
                let retire_map = &mut retire_map_list[retire_idx.as_usize()];

                retire_map.file_id = retire_file_id;
                retire_map.next_block_nr = retire_next_block_nr;
                retire_map.next_idx = retire_next_idx;

                self.confirm_add_tail(bag, retire_block_nr, retire_idx);
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
                Ok(block) => block.cast_array::<RawWordMap>(),
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

impl Debug for WordMap {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WordMap")
            .field("bag_nr", &self.bag_nr)
            .field("last_head_nr", &RefSlice(&self.last_head_nr, 0))
            .field("last_head_idx", &RefSlice(&self.last_head_idx, 0))
            .field("last_tail_nr", &RefSlice(&self.last_tail_nr, 0))
            .field("last_tail_idx", &RefSlice(&self.last_tail_idx, 0))
            .finish()?;

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
                            write!(f, "{:8?} ", self.0[i])?;
                        }
                    }
                }
                Ok(())
            }
        }

        Ok(())
    }
}
