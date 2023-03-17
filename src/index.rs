use std::collections::HashSet;
use std::path::{Path, PathBuf};

static STOPWORDS: [&str; 1] = ["a"];

///
///
pub struct Words {
    pub words: Vec<String>,
    pub file_idx: Vec<HashSet<usize>>,
    pub files: Vec<PathBuf>,
}

impl Words {
    pub fn new() -> Self {
        Words {
            words: vec![],
            file_idx: vec![],
            files: vec![],
        }
    }

    pub fn merge(&mut self, words: Words) -> Result<(), ()> {
        // for (i, w) in words.words.into_iter().enumerate().rev() {
        //     let idx = match self.words.binary_search(&w) {
        //         Ok(v) => v,
        //         Err(v) => {
        //             self.words.insert(v, w);
        //             self.file_idx.insert(v, HashSet::new());
        //             v
        //         }
        //     };
        //
        //     let Some(file_idx) = self.file_idx.get_mut(v) else {
        //         return Err(());
        //     };
        // }

        Ok(())
    }
}

pub fn index_txt(_path: &Path) -> Words {
    let mut words = Words::new();

    words
}
