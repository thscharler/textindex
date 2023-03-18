use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::path::{Path, PathBuf};
use std::{fs, io};

static STOPWORDS: [&str; 1] = ["a"];

///
///
pub struct Words {
    pub words: Vec<String>,
    pub word_count: Vec<usize>,
    pub file_idx: Vec<HashSet<usize>>,
    pub files: Vec<PathBuf>,
}

impl Debug for Words {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        for (word_idx, word) in self.words.iter().enumerate() {
            write!(f, "{}|{}|", word, self.word_count[word_idx])?;
            for file_idx in self.file_idx[word_idx].iter() {
                write!(f, "{}/", file_idx)?;
            }
            writeln!(f)?;
        }
        Ok(())
    }
}

impl Words {
    pub fn new() -> Self {
        Words {
            words: vec![],
            word_count: vec![],
            file_idx: vec![],
            files: vec![],
        }
    }

    pub fn add_file(&mut self, file: PathBuf) -> usize {
        let file = file.to_path_buf();
        match self.files.binary_search(&file) {
            Ok(idx) => idx,
            Err(idx) => {
                self.files.insert(idx, file);
                idx
            }
        }
    }

    pub fn add_single<S: AsRef<str> + Into<String>>(&mut self, word: S, file_idx: usize) -> usize {
        self.add_word(word, 1, &[file_idx])
    }

    pub fn add_word<S: AsRef<str> + Into<String>>(
        &mut self,
        word: S,
        count: usize,
        file_idx: &[usize],
    ) -> usize {
        match self
            .words
            .binary_search_by(|probe| probe.as_str().cmp(word.as_ref()))
        {
            Ok(idx) => {
                self.word_count[idx] += count;
                self.file_idx[idx].extend(file_idx);
                idx
            }
            Err(idx) => {
                self.words.insert(idx, word.into());
                self.word_count.insert(idx, count);
                self.file_idx.insert(idx, HashSet::new());
                self.file_idx[idx].extend(file_idx);
                idx
            }
        }
    }

    pub fn merge(&mut self, words: Words) {
        let mut map_fileidx = Vec::new();

        let add = words.words.len();
        self.words.reserve(add);
        self.word_count.reserve(add);
        self.file_idx.reserve(add);
        self.files.reserve(words.files.len());

        for file in words.files.into_iter() {
            let idx = self.add_file(file);
            map_fileidx.push(idx);
        }

        for (word_idx, word) in words.words.into_iter().enumerate() {
            let files = words.file_idx[word_idx]
                .iter()
                .map(|v| map_fileidx[*v])
                .collect::<Vec<_>>();
            let count = words.word_count[word_idx];

            self.add_word(word, count, &files);
        }
    }
}

pub fn index_txt(path: &Path, buf: &Vec<u8>) -> Result<Words, io::Error> {
    let mut words = Words::new();

    let file_idx = words.add_file(path.into());
    let buf = fs::read_to_string(path)?;

    // split at white
    for w in buf.split_whitespace() {
        let w = w.trim_end_matches('"');
        let w = w.trim_end_matches('\'');
        let w = w.trim_end_matches('?');
        let w = w.trim_end_matches('.');
        let w = w.trim_end_matches(',');
        let w = w.trim_start_matches('"');

        let w = w.to_lowercase();

        words.add_single(w, file_idx);
    }

    Ok(words)
}
