use crate::index2::{BlkIdx, FileId, IndexError, WordBlockType, WordFileBlocks, BLOCK_SIZE};
use blockfile2::LogicalNr;
use std::collections::BTreeMap;
use std::fmt::Debug;

#[derive(Debug)]
pub struct FileList {
    last_file_id: FileId,
    last_block_nr: LogicalNr,
    last_block_idx: BlkIdx,
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

        let mut last_file_id = FileId(0u32);
        let mut last_block_nr = LogicalNr(0u32);
        let mut last_block_idx = BlkIdx(0u32);

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
                let file_id = FileId(u32::from_ne_bytes(file_id));
                if file_id == 0 {
                    last_block_nr = block_nr;
                    last_block_idx = BlkIdx(idx as u32);
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
                        block_idx: BlkIdx(idx as u32),
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
                    self.last_block_idx = BlkIdx(0);
                }

                assert!(file_data.name.len() < 256);

                let file_name = file_data.name.as_bytes();

                let mut buf: Vec<u8> = Vec::new();
                buf.extend(file_id.0.to_ne_bytes());
                buf.extend((file_name.len() as u8).to_ne_bytes());
                buf.extend(file_name);

                let mut block = db.get_mut(self.last_block_nr)?;
                let mut idx = self.last_block_idx.as_usize();
                if idx + buf.len() > BLOCK_SIZE {
                    block = db.alloc(Self::TY)?;
                    self.last_block_nr = block.block_nr();
                    self.last_block_idx = BlkIdx(0);
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
                block_nr: LogicalNr(0),
                block_idx: BlkIdx(0),
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
