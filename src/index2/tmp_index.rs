use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::time::{Duration, Instant};

#[derive(Debug)]
pub struct TmpWords {
    pub file: String,
    pub words: HashMap<String, usize>,
    pub count: usize,
}

impl TmpWords {
    pub fn new<S: Into<String>>(path: S) -> Self {
        Self {
            file: path.into(),
            words: Default::default(),
            count: 0,
        }
    }

    pub fn add_word<S: AsRef<str>>(&mut self, word: S) {
        if self.words.contains_key(word.as_ref()) {
            *self.words.get_mut(word.as_ref()).expect("word") += 1;
        } else {
            self.words.insert(word.as_ref().to_string(), 1);
        }

        self.count += 1;
    }

    pub fn invert(&self) -> BTreeMap<usize, Vec<String>> {
        let mut r = BTreeMap::new();
        for (k, v) in &self.words {
            r.entry(*v)
                .and_modify(|v: &mut Vec<String>| v.push(k.clone()))
                .or_insert(vec![k.clone()]);
        }
        r
    }
}
