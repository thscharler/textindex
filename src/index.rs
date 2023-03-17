use std::collections::HashSet;
use std::path::{Path, PathBuf};

static STOPWORDS: [&str; 1] = ["a"];

///
///
pub struct Words {
    pub words: Vec<String>,
    pub fileidx: Vec<HashSet<usize>>,
    pub files: Vec<PathBuf>,
}

impl Words {
    pub fn new() -> Self {
        Words {
            words: vec![],
            fileidx: vec![],
            files: vec![],
        }
    }
}

pub fn index_txt(_path: &Path) -> Words {
    let mut words = Words::new();

    words
}
