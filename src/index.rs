use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::mem;
use std::time::Instant;
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
    pub files: Vec<String>,
    pub age: Instant,
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
            age: Instant::now(),
        }
    }

    pub fn reserve(&mut self, n_words: usize, n_files: usize) {
        self.words.reserve(n_words);
        self.word_count.reserve(n_words);
        self.file_idx.reserve(n_words);
        self.files.reserve(n_files);
    }

    pub fn add_file(&mut self, file: String) -> usize {
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

    fn check_sorted(what: &str, vec: &Vec<String>) {
        let mut it = vec.iter();
        let mut v0 = it.next();
        while v0.is_some() {
            let v1 = it.next();

            if v1.is_none() {
                break;
            }
            if v0 >= v1 {
                println!("not sorted {} {:?} >= {:?}", what, v0, v1);
                break;
            }

            v0 = v1;
        }
    }

    pub fn append(&mut self, other: Words) -> (usize, usize) {
        let mut upd = 0;
        let mut ins = 0;

        let mut map_fileidx = Vec::new();
        for file in other.files.into_iter() {
            let idx = self.add_file(file);
            map_fileidx.push(idx);
        }

        for ((a_word, a_count), a_file_idx) in (other.words.into_iter().rev())
            .zip(other.word_count.into_iter().rev())
            .zip(other.file_idx.into_iter().rev())
        {
            match self.words.binary_search(&a_word) {
                Ok(s_idx) => {
                    upd += 1;
                    self.word_count[s_idx] += a_count;
                    self.file_idx[s_idx].extend(a_file_idx.into_iter())
                }
                Err(s_idx) => {
                    ins += 1;
                    self.words.insert(s_idx, a_word);
                    self.word_count.insert(s_idx, a_count);
                    self.file_idx.insert(s_idx, a_file_idx);
                }
            }
        }

        (upd, ins)
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
            one.files.into_iter().enumerate(),
            other.files.into_iter().enumerate(),
            |(_i_idx, i)| {
                let idx = words.borrow_mut().add_file(i);
                file_i.borrow_mut().push(idx);
            },
            |(_j_idx, j)| {
                let idx = words.borrow_mut().add_file(j);
                file_j.borrow_mut().push(idx);
            },
            |(_i_idx, i), _| {
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

fn iter_merge<T: Ord + Debug>(
    mut it: impl Iterator<Item = (usize, T)>,
    mut jt: impl Iterator<Item = (usize, T)>,
    merge_i: impl Fn((usize, T)),
    merge_j: impl Fn((usize, T)),
    both: impl Fn((usize, T), (usize, T)),
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
            let Some((_, i_test)) = &i else {
                unreachable!()
            };
            let Some((_, j_test)) = &j else {
                unreachable!()
            };

            match i_test.cmp(&j_test) {
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
    for w in buf.split(|c: char| {
        c as usize <= 32
            || c == '_'
            || c == ','
            || c == '.'
            || c == '='
            || c == '\u{FFFD}'
            || c.is_whitespace()
    }) {
        let w = w.trim_end_matches(|c: char| {
            c == '"'
                || c == '\''
                || c == '`'
                || c == '?'
                || c == '!'
                || c == ';'
                || c == ':'
                || c == '.'
                || c == ','
                || c == '@'
                || c == '#'
                || c == '-'
                || c == '+'
                || c == '*'
                || c == '~'
                || c == '('
                || c == ')'
                || c == '['
                || c == ']'
        });
        let w = w.trim_start_matches(|c: char| {
            c == '"'
                || c == '\''
                || c == '`'
                || c == '?'
                || c == '!'
                || c == ';'
                || c == ':'
                || c == '.'
                || c == ','
                || c == '@'
                || c == '#'
                || c == '-'
                || c == '+'
                || c == '*'
                || c == '~'
                || c == '('
                || c == ')'
                || c == '['
                || c == ']'
        });

        if let Some(c) = w.chars().next() {
            if c.is_ascii_digit() {
                continue;
            } else if c == '<' {
                continue;
            } else if c == '&' {
                continue;
            } else if c == '/' {
                continue;
            }
        }

        if w.is_empty() {
            continue;
        }

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
