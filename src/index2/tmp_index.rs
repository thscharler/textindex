use crate::proc3::stop_words::STOP_WORDS;
use std::collections::BTreeMap;
use std::fmt::Debug;

#[derive(Debug)]
pub struct TmpWords {
    pub file: String,
    pub words: BTreeMap<String, usize>,
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
        if STOP_WORDS
            .binary_search_by(|probe| (*probe).cmp(word.as_ref()))
            .is_ok()
        {
            return;
        }

        // spurios tags
        if word.as_ref().contains('<') || word.as_ref().contains(">") {
            return;
        }

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
