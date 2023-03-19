use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::mem;
use std::path::PathBuf;
use tl::ParserOptions;

const STOP_WORDS: [&str; 35] = [
    "a", "all", "and", "as", "at", "but", "could", "for", "from", "had", "he", "her", "him", "his",
    "hot", "i", "in", "into", "it", "me", "my", "of", "on", "she", "so", "that", "the", "then",
    "to", "up", "was", "were", "with", "you", "your",
];

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

    pub fn reserve(&mut self, n_words: usize, n_files: usize) {
        self.words.reserve(n_words);
        self.word_count.reserve(n_words);
        self.file_idx.reserve(n_words);
        self.files.reserve(n_files);
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

    pub fn add_word<S: AsRef<str> + Into<String>>(&mut self, word: S, file_idx: usize) {
        if let Ok(_) = STOP_WORDS.binary_search_by(|probe| (*probe).cmp(word.as_ref())) {
            return;
        }

        match self
            .words
            .binary_search_by(|probe| probe.as_str().cmp(word.as_ref()))
        {
            Ok(idx) => {
                self.word_count.get_mut(idx).map(|v| {
                    *v += 1;
                });
                self.file_idx.get_mut(idx).map(|v| {
                    v.insert(file_idx);
                });
            }
            Err(idx) => {
                self.words.insert(idx, word.into());
                self.word_count.insert(idx, 1);
                self.file_idx.insert(idx, HashSet::new());
                self.file_idx[idx].insert(file_idx);
            }
        }
    }

    pub fn merge(&mut self, other: Words) {
        let one = mem::replace(self, Words::new());

        let len_w = one.words.len() + other.words.len();
        let len_f = one.files.len() + other.files.len();
        self.reserve(len_w, len_f);

        let words = RefCell::new(Words::new());
        let file_i = RefCell::new(Vec::new());
        let file_j = RefCell::new(Vec::new());

        iter_merge(
            one.files.into_iter(),
            other.files.into_iter(),
            |i| {
                let idx = words.borrow_mut().add_file(i);
                file_i.borrow_mut().push(idx);
            },
            |j| {
                let idx = words.borrow_mut().add_file(j);
                file_j.borrow_mut().push(idx);
            },
            |i, _j| {
                let idx = words.borrow_mut().add_file(i);
                file_i.borrow_mut().push(idx);
                file_j.borrow_mut().push(idx);
            },
        );

        iter_merge(
            one.words.into_iter().enumerate(),
            other.words.into_iter().enumerate(),
            |(i_idx, i)| {
                let mut words = words.borrow_mut();
                words.words.push(i);
                words.word_count.push(one.word_count[i_idx]);
                words.file_idx.push(
                    one.file_idx[i_idx]
                        .iter()
                        .map(|idx| file_i.borrow()[*idx])
                        .collect(),
                );
            },
            |(j_idx, j)| {
                let mut words = words.borrow_mut();
                words.words.push(j);
                words.word_count.push(other.word_count[j_idx]);
                words.file_idx.push(
                    other.file_idx[j_idx]
                        .iter()
                        .map(|idx| file_j.borrow()[*idx])
                        .collect(),
                );
            },
            |(i_idx, i), (j_idx, _j)| {
                let mut words = words.borrow_mut();
                words.words.push(i);
                words
                    .word_count
                    .push(one.word_count[i_idx] + other.word_count[j_idx]);

                words.file_idx.push(
                    (one.file_idx[i_idx].iter().map(|idx| file_i.borrow()[*idx]))
                        .chain(
                            other.file_idx[j_idx]
                                .iter()
                                .map(|idx| file_j.borrow()[*idx]),
                        )
                        .collect(),
                );
            },
        );

        let _ = mem::replace(self, words.into_inner());
    }
}

fn iter_merge<T: Ord>(
    mut it: impl Iterator<Item = T>,
    mut jt: impl Iterator<Item = T>,
    merge_i: impl Fn(T),
    merge_j: impl Fn(T),
    both: impl Fn(T, T),
) {
    let mut i = None;
    let mut j = None;
    loop {
        if i.is_none() {
            i = it.next();
        }
        if j.is_none() {
            j = jt.next();
        }

        if i.is_none() && j.is_none() {
            break;
        } else if i.is_some() && j.is_some() {
            match i.cmp(&j) {
                Ordering::Less => {
                    let Some(i_val) = i else {
                        unreachable!();
                    };
                    merge_i(i_val);
                    i = None;
                }
                Ordering::Greater => {
                    let Some(j_val) = j else {
                        unreachable!();
                    };
                    merge_j(j_val);
                    j = None;
                }
                Ordering::Equal => {
                    let Some(i_val) = i else {
                        unreachable!();
                    };
                    let Some(j_val) = j else {
                        unreachable!();
                    };
                    both(i_val, j_val);
                    i = None;
                    j = None;
                }
            }
        } else if let Some(i_val) = i {
            merge_i(i_val);
            i = None;
        } else if let Some(j_val) = j {
            merge_j(j_val);
            j = None;
        }
    }
}

pub fn index_txt(words: &mut Words, file_idx: usize, buf: &str) {
    // split at white
    for w in buf.split_whitespace() {
        let w = w.trim_end_matches('"');
        let w = w.trim_end_matches('\'');
        let w = w.trim_end_matches('?');
        let w = w.trim_end_matches('!');
        let w = w.trim_end_matches(';');
        let w = w.trim_end_matches('.');
        let w = w.trim_end_matches(',');
        let w = w.trim_end_matches(')');
        let w = w.trim_start_matches('"');
        let w = w.trim_start_matches('\'');
        let w = w.trim_start_matches('(');

        let w = w.to_lowercase();

        words.add_word(w, file_idx);
    }
}

pub fn index_html(words: &mut Words, file_idx: usize, buf: &str) -> Result<(), tl::ParseError> {
    let dom = tl::parse(buf, ParserOptions::new())?;
    for node in dom.nodes() {
        if let Some(tag) = node.as_tag() {
            if tag.name() != "style" && tag.name() != "script" {
                let txt = node.inner_text(dom.parser());
                index_txt(words, file_idx, txt.as_ref());
            }
        }
    }
    Ok(())
}
