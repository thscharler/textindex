use crate::error::AppError;
use html5ever::interface::{ElementFlags, NodeOrText, QuirksMode, TreeSink};
use html5ever::tendril::{StrTendril, TendrilSink};
use html5ever::{parse_document, Attribute, ExpandedName, ParseOpts, QualName};
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Formatter};
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::str::from_utf8;
use std::time::{Duration, Instant};

const STOP_WORDS: [&str; 35] = [
    "a", "all", "and", "as", "at", "but", "could", "for", "from", "had", "he", "her", "him", "his",
    "hot", "i", "in", "into", "it", "me", "my", "of", "on", "she", "so", "that", "the", "then",
    "to", "up", "was", "were", "with", "you", "your",
];

///
pub struct Words {
    pub words: BTreeMap<String, Word>,
    pub files: Vec<String>,
    pub age: Instant,
    pub auto_save: Duration,
}

pub struct Word {
    pub count: u32,
    pub file_idx: BTreeSet<u32>,
}

impl Debug for Words {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        for (txt, word) in self.words.iter() {
            write!(f, "{}|{}|", txt, word.count)?;
            // for file_idx in word.file_idx.iter() {
            //     write!(f, "{}/", file_idx)?;
            // }
            writeln!(f)?;
        }
        Ok(())
    }
}

impl Word {
    pub fn add_file_idx(&mut self, f_idx: u32) {
        self.file_idx.insert(f_idx);
    }
}

impl Words {
    pub fn new() -> Self {
        Words {
            words: Default::default(),
            files: Default::default(),
            age: Instant::now(),
            auto_save: Duration::from_secs(60),
        }
    }

    pub fn write(&self, path: &Path) -> Result<(), AppError> {
        let mut f = BufWriter::new(File::create(path)?);

        f.write_all(&(self.files.len() as u32).to_ne_bytes())?;
        for file in self.files.iter() {
            f.write_all(file.as_bytes())?;
            f.write_all(b"\n")?;
        }

        f.write_all(&(self.words.len() as u32).to_ne_bytes())?;
        for (txt, word) in self.words.iter() {
            f.write_all(txt.as_bytes())?;
            f.write_all(b"\n")?;
            f.write_all(&word.count.to_ne_bytes())?;

            f.write_all(&(word.file_idx.len() as u32).to_ne_bytes())?;
            for u in &word.file_idx {
                f.write_all(&u.to_ne_bytes())?;
            }
            f.write_all(b"\n")?;
        }

        Ok(())
    }

    pub fn read(path: &Path) -> Result<Words, AppError> {
        let mut words = Words::new();

        let mut f = BufReader::new(File::open(path)?);
        let mut buf = Vec::new();
        let mut u = [0u8; 4];
        let mut b = [0u8; 1];

        f.read_exact(&mut u)?;
        let n = u32::from_ne_bytes(u) as usize;
        for _ in 0..n {
            buf.clear();

            f.read_until(b'\n', &mut buf)?;
            buf.pop();
            let file = from_utf8(&buf)?.to_string();
            words.files.push(file);
        }

        f.read_exact(&mut u)?;
        let n = u32::from_ne_bytes(u) as usize;
        for _ in 0..n {
            buf.clear();

            f.read_until(b'\n', &mut buf)?;
            buf.pop();
            let word = from_utf8(&buf)?.to_string();

            f.read_exact(&mut u)?;
            let count = u32::from_ne_bytes(u);

            let mut file_idx = BTreeSet::new();
            f.read_exact(&mut u)?;
            let n = u32::from_ne_bytes(u);
            for _ in 0..n {
                f.read_exact(&mut u)?;
                let idx = u32::from_ne_bytes(u);
                file_idx.insert(idx);
            }
            f.read_exact(&mut b)?;

            words.words.insert(word, Word { count, file_idx });
        }

        Ok(words)
    }

    pub fn add_file(&mut self, file: String) -> u32 {
        match self.files.iter().position(|v| &file == v) {
            None => {
                let idx = self.files.len();
                self.files.push(file);
                idx as u32
            }
            Some(idx) => idx as u32,
        }
    }

    pub fn remove_file(&mut self, file: String) {
        if let Ok(idx) = self.files.binary_search(&file) {
            self.files.remove(idx);

            for word in self.words.values_mut() {
                word.file_idx.remove(&(idx as u32));
            }

            self.words.retain(|_txt, word| !word.file_idx.is_empty());
        }
    }

    pub fn add_word<S: AsRef<str> + Into<String>>(&mut self, word: S, file_idx: u32) {
        if let Ok(_) = STOP_WORDS.binary_search_by(|probe| (*probe).cmp(word.as_ref())) {
            return;
        }

        self.words
            .entry(word.into())
            .and_modify(|v| {
                v.count += 1;
                v.add_file_idx(file_idx)
            })
            .or_insert_with(|| Word {
                count: 1,
                file_idx: {
                    let mut v = BTreeSet::new();
                    v.insert(file_idx);
                    v
                },
            });
    }

    pub fn append(&mut self, other: Words) {
        let mut map_fileidx = Vec::new();
        for file in other.files.into_iter() {
            let idx = self.add_file(file);
            map_fileidx.push(idx);
        }

        for (a_txt, a_word) in other.words.into_iter() {
            self.words
                .entry(a_txt)
                .and_modify(|v| {
                    v.count += a_word.count;
                    for f_idx in &a_word.file_idx {
                        v.add_file_idx(map_fileidx[*f_idx as usize]);
                    }
                })
                .or_insert(a_word);
        }
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
                || c == '^'
                || c == '('
                || c == ')'
                || c == '['
                || c == ']'
                || c == '{'
                || c == '}'
                || c == '|'
                || c == '\\'
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
                || c == '^'
                || c == '('
                || c == ')'
                || c == '['
                || c == ']'
                || c == '{'
                || c == '}'
                || c == '|'
                || c == '\\'
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

pub fn index_html(words: &mut Words, file_idx: u32, buf: &str) {
    struct IdxSink<'a> {
        pub words: &'a mut Words,
        pub file_idx: u32,

        pub elem: Vec<QualName>,
        pub comment: Vec<StrTendril>,
        pub pi: Vec<(StrTendril, StrTendril)>,
    }

    #[derive(Clone)]
    enum IdxHandle {
        Elem(usize),
        Comment(usize),
        Pi(usize),
    }

    impl<'a> TreeSink for IdxSink<'a> {
        type Handle = IdxHandle;
        type Output = ();

        fn finish(self) -> Self::Output {}

        fn parse_error(&mut self, _msg: Cow<'static, str>) {
            // println!("parse_error {:?}", msg);
        }

        fn get_document(&mut self) -> Self::Handle {
            IdxHandle::Elem(0)
        }

        fn elem_name<'b>(&'b self, target: &'b Self::Handle) -> ExpandedName<'b> {
            match target {
                IdxHandle::Elem(i) => self.elem[*i].expanded(),
                IdxHandle::Comment(_) => {
                    unimplemented!()
                }
                IdxHandle::Pi(_) => {
                    unimplemented!()
                }
            }
        }

        fn create_element(
            &mut self,
            name: QualName,
            _attrs: Vec<Attribute>,
            _flags: ElementFlags,
        ) -> Self::Handle {
            // println!("create_element {:?} {:?}", name, attrs);

            let handle = self.elem.len();
            self.elem.push(name);

            IdxHandle::Elem(handle)
        }

        fn create_comment(&mut self, text: StrTendril) -> Self::Handle {
            // println!("create_comment {:?}", text);

            let handle = self.comment.len();
            self.comment.push(text);

            IdxHandle::Comment(handle)
        }

        fn create_pi(&mut self, target: StrTendril, data: StrTendril) -> Self::Handle {
            // println!("create_pi {:?} {:?}", target, data);

            let handle = self.pi.len();
            self.pi.push((target, data));

            IdxHandle::Pi(handle)
        }

        fn append(&mut self, _parent: &Self::Handle, child: NodeOrText<Self::Handle>) {
            match child {
                NodeOrText::AppendNode(_) => {}
                NodeOrText::AppendText(v) => {
                    index_txt(self.words, self.file_idx, v.as_ref());
                }
            }
        }

        fn append_based_on_parent_node(
            &mut self,
            _element: &Self::Handle,
            _prev_element: &Self::Handle,
            _child: NodeOrText<Self::Handle>,
        ) {
            // match child {
            //     NodeOrText::AppendNode(v) => {}
            //     NodeOrText::AppendText(v) => {
            //         println!("append_based_on_parent_node {:?}", v);
            //     }
            // }
        }

        fn append_doctype_to_document(
            &mut self,
            _name: StrTendril,
            _public_id: StrTendril,
            _system_id: StrTendril,
        ) {
            // println!(
            //     "append_doctype_to_document {:?} {:?} {:?}",
            //     name, public_id, system_id
            // );
        }

        fn get_template_contents(&mut self, target: &Self::Handle) -> Self::Handle {
            target.clone()
        }

        fn same_node(&self, _x: &Self::Handle, _y: &Self::Handle) -> bool {
            false
        }

        fn set_quirks_mode(&mut self, _mode: QuirksMode) {}

        fn append_before_sibling(
            &mut self,
            _sibling: &Self::Handle,
            _new_node: NodeOrText<Self::Handle>,
        ) {
            // match new_node {
            //     NodeOrText::AppendNode(v) => {}
            //     NodeOrText::AppendText(v) => {
            //         println!("append_before_sibling {:?}", v);
            //     }
            // }
        }

        fn add_attrs_if_missing(&mut self, _target: &Self::Handle, _attrs: Vec<Attribute>) {}

        fn remove_from_parent(&mut self, _target: &Self::Handle) {}

        fn reparent_children(&mut self, _node: &Self::Handle, _new_parent: &Self::Handle) {}
    }

    let s = IdxSink {
        words,
        file_idx,
        elem: vec![],
        comment: vec![],
        pi: vec![],
    };

    let p = parse_document(s, ParseOpts::default());
    p.one(buf);
}

// pub fn index_html(words: &mut Words, file_idx: u32, buf: &str) -> Result<(), tl::ParseError> {
//     let dom = tl::parse(buf, ParserOptions::new())?;
//     for node in dom.nodes() {
//         if let Some(tag) = node.as_tag() {
//             if tag.name() != "style" && tag.name() != "script" {
//                 let txt = node.inner_text(dom.parser());
//                 index_txt(words, file_idx, txt.as_ref());
//             }
//         }
//     }
//     Ok(())
// }
