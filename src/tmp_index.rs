use html5ever::interface::{ElementFlags, NodeOrText, QuirksMode, TreeSink};
use html5ever::tendril::{StrTendril, TendrilSink};
use html5ever::{parse_document, Attribute, ExpandedName, ParseOpts, QualName};
use std::borrow::Cow;
use std::collections::BTreeSet;
use std::fmt::Debug;
use std::io::BufRead;

const STOP_WORDS: [&str; 35] = [
    "a", "all", "and", "as", "at", "but", "could", "for", "from", "had", "he", "her", "him", "his",
    "hot", "i", "in", "into", "it", "me", "my", "of", "on", "she", "so", "that", "the", "then",
    "to", "up", "was", "were", "with", "you", "your",
];

#[derive(Debug)]
pub struct TmpWords {
    pub file: String,
    pub words: BTreeSet<String>,
}

impl TmpWords {
    pub fn new<S: Into<String>>(path: S) -> Self {
        Self {
            file: path.into(),
            words: Default::default(),
        }
    }

    pub fn add_word<S: Into<String>>(&mut self, word: S) {
        let word = word.into();

        if let Ok(_) = STOP_WORDS.binary_search_by(|probe| (*probe).cmp(word.as_str())) {
            return;
        }

        self.words.insert(word);
    }
}

pub fn index_txt(words: &mut TmpWords, buf: &str) {
    // split at white
    for word in buf.split(|c: char| {
        c as u32 <= 32
            || c == '_'
            || c == ','
            || c == '.'
            || c == '='
            || c == '/'
            || c == '\u{FFFD}'
            || c.is_whitespace()
    }) {
        let word = word.trim_end_matches(|c: char| {
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
        let word = word.trim_start_matches(|c: char| {
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

        if let Some(c) = word.chars().next() {
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

        if word.is_empty() {
            continue;
        }

        let c = word.chars().next().expect("char");
        if c.is_ascii_alphanumeric() {
            let word = word.to_lowercase();
            words.add_word(word);
        }
    }
}

pub fn index_html(words: &mut TmpWords, buf: &str) {
    struct IdxSink<'a> {
        pub words: &'a mut TmpWords,

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

        fn elem_name<'c>(&'c self, target: &'c Self::Handle) -> ExpandedName<'c> {
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
                    index_txt(self.words, v.as_ref());
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
        elem: vec![],
        comment: vec![],
        pi: vec![],
    };

    let p = parse_document(s, ParseOpts::default());
    p.one(buf);
}
