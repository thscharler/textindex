use crate::index2::{BlkIdx, FileId, IndexError, WordBlockType, WordFileBlocks};
use blockfile2::{BlockRead, BlockWrite, LogicalNr};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::{Read, Write};

#[derive(Debug)]
pub struct FileList {
    last_file_id: FileId,
    last_block_nr: LogicalNr,
    list: BTreeMap<FileId, FileData>,
}

#[derive(Debug)]
pub struct FileData {
    pub name: String,
    pub block_nr: LogicalNr,
    pub block_idx: BlkIdx,
}

impl FileList {
    pub(crate) const TY: WordBlockType = WordBlockType::FileList;

    pub(crate) fn recover(db: &mut WordFileBlocks) -> Result<FileList, IndexError> {
        Self::load(db)
    }

    pub(crate) fn load(db: &mut WordFileBlocks) -> Result<FileList, IndexError> {
        let mut list = BTreeMap::new();
        let mut last_file_id = FileId(0u32);
        let mut last_block_nr = LogicalNr(0u32);

        let mut r = db.read_stream(Self::TY)?;
        loop {
            let block_nr = r.block_nr();
            let block_idx = BlkIdx(r.idx() as u32);

            let mut buf_file_id = [0u8; 4];
            if !r.read_maybe(&mut buf_file_id)? {
                last_block_nr = block_nr;
                break;
            }
            let file_id = FileId(u32::from_ne_bytes(buf_file_id));
            if file_id == 0 {
                last_block_nr = block_nr;
                break;
            }
            last_file_id = file_id;

            let mut buf_name_len = [0u8; 2];
            r.read_exact(&mut buf_name_len)?;
            let name_len = u16::from_ne_bytes(buf_name_len);

            let mut buf_name = Vec::with_capacity(name_len as usize);
            buf_name.resize(name_len as usize, 0);
            r.read_exact(buf_name.as_mut())?;
            let name = String::from_utf8(buf_name)?;

            list.insert(
                file_id,
                FileData {
                    name,
                    block_nr,
                    block_idx,
                },
            );
        }

        Ok(Self {
            last_file_id,
            last_block_nr,
            list,
        })
    }

    pub(crate) fn store(&mut self, db: &mut WordFileBlocks) -> Result<(), IndexError> {
        // assume append only
        let mut w = db.append_stream(Self::TY)?;

        let mut buf: Vec<u8> = Vec::new();
        for (file_id, file_data) in self.list.iter_mut() {
            if file_data.block_nr == 0 {
                file_data.block_nr = w.block_nr();
                file_data.block_idx = BlkIdx(w.idx() as u32);

                assert!(file_data.name.len() < 65536);

                let file_name = file_data.name.as_bytes();

                buf.clear();
                buf.extend(file_id.0.to_ne_bytes());
                buf.extend((file_name.len() as u16).to_ne_bytes());
                buf.extend(file_name);

                w.write_all(buf.as_slice())?;
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
