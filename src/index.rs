use std::fmt::{Debug, Formatter};
use std::time::Instant;
use tl::ParserOptions;

const STOP_WORDS: [&str; 35] = [
    "a", "all", "and", "as", "at", "but", "could", "for", "from", "had", "he", "her", "him", "his",
    "hot", "i", "in", "into", "it", "me", "my", "of", "on", "she", "so", "that", "the", "then",
    "to", "up", "was", "were", "with", "you", "your",
];

///
pub struct Words {
    pub words: Vec<String>,
    pub word_count: Vec<u32>,
    pub file_idx: Vec<Vec<u32>>,
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

    pub fn add_file(&mut self, file: String) -> u32 {
        match self.files.binary_search(&file) {
            Ok(idx) => idx as u32,
            Err(idx) => {
                self.files.insert(idx, file);
                idx as u32
            }
        }
    }

    pub fn remove_file(&mut self, file: String) -> Option<u32> {
        match self.files.binary_search(&file) {
            Ok(idx) => {
                self.files.remove(idx);

                for file_idx in self.file_idx.iter_mut() {
                    match file_idx.binary_search(&(idx as u32)) {
                        Ok(idx) => {
                            file_idx.remove(idx);
                        }
                        Err(_) => {}
                    }
                }

                for w_idx in (0usize..self.words.len()).rev() {
                    if self.file_idx[w_idx].is_empty() {
                        self.file_idx.remove(w_idx);
                        self.word_count.remove(w_idx);
                        self.words.remove(w_idx);
                    }
                }

                Some(idx as u32)
            }
            Err(_) => None,
        }
    }

    fn add_file_idx(&mut self, w_idx: usize, f_idx: u32) {
        let file_idx = &mut self.file_idx[w_idx];
        match file_idx.binary_search(&f_idx) {
            Ok(_) => {
                // noop
            }
            Err(i) => {
                file_idx.insert(i, f_idx);
            }
        }
    }

    pub fn add_word<S: AsRef<str> + Into<String>>(&mut self, word: S, file_idx: u32) {
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
                self.add_file_idx(idx, file_idx);
            }
            Err(idx) => {
                self.words.insert(idx, word.into());
                self.word_count.insert(idx, 1);
                self.file_idx.insert(idx, Vec::new());
                self.add_file_idx(idx, file_idx);
            }
        }
    }

    pub fn append(&mut self, other: Words) -> (u32, u32) {
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
                    for f_idx in a_file_idx {
                        self.add_file_idx(s_idx, f_idx);
                    }
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
}

pub fn index_txt(words: &mut Words, file_idx: u32, buf: &str) {
    // split at white
    for w in buf.split(|c: char| {
        c as u32 <= 32
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

pub fn index_html(words: &mut Words, file_idx: u32, buf: &str) -> Result<(), tl::ParseError> {
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
