use html5ever::interface::{ElementFlags, NodeOrText, QuirksMode, TreeSink};
use html5ever::tendril::{StrTendril, TendrilSink};
use html5ever::{parse_document, Attribute, ExpandedName, ParseOpts, QualName};
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;

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

    pub fn add_word<S: AsRef<str>>(&mut self, word: S) {
        if let Ok(_) = STOP_WORDS.binary_search_by(|probe| (*probe).cmp(word.as_ref())) {
            return;
        }

        if !self.words.contains(word.as_ref()) {
            self.words.insert(word.as_ref().to_string());
        }
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
    #[derive(Debug)]
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
            // println!("parse_error {:?} {:?}", _msg, self);
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
            // println!("create_element {:?} {:?}", name, _attrs);

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

pub const STOP_WORDS: &[&str] = &[
    "a",
    "about",
    "after",
    "again",
    "agin",
    "ain't",
    "all",
    "all",
    "almost",
    "alt",
    "always",
    "an",
    "and",
    "another",
    "any",
    "anyway",
    "are",
    "around",
    "as",
    "asked",
    "at",
    "author",
    "away",
    "back",
    "be",
    "because",
    "been",
    "before",
    "behind",
    "being",
    "best",
    "better",
    "between",
    "both",
    "but",
    "by",
    "cain't",
    "came",
    "can",
    "can't",
    "com",
    "comes",
    "could",
    "couldn't",
    "day",
    "did",
    "didn't",
    "do",
    "don't",
    "done",
    "down",
    "each",
    "else",
    "even",
    "ever",
    "every",
    "few",
    "finally",
    "find",
    "finds",
    "for",
    "found",
    "from",
    "get",
    "gets",
    "give",
    "go",
    "going",
    "gonna",
    "good",
    "got",
    "gotta",
    "had",
    "has",
    "have",
    "he",
    "he's",
    "her",
    "here",
    "herself",
    "him",
    "himself",
    "his",
    "hot",
    "how",
    "i",
    "i'd",
    "i'll",
    "i'm",
    "if",
    "in",
    "inside",
    "into",
    "is",
    "it",
    "it's",
    "its",
    "just",
    "keep",
    "knew",
    "know",
    "knowed",
    "last",
    "leave",
    "left",
    "like",
    "look",
    "looked",
    "looks",
    "made",
    "make",
    "makes",
    "making",
    "many",
    "may",
    "maybe",
    "me",
    "mean",
    "meets",
    "might",
    "more",
    "most",
    "much",
    "must",
    "my",
    "myself",
    "need",
    "net",
    "never",
    "new",
    "next",
    "no",
    "not",
    "nothin",
    "now",
    "of",
    "off",
    "ok",
    "on",
    "one",
    "only",
    "onto",
    "or",
    "other",
    "our",
    "out",
    "over",
    "own",
    "part",
    "please",
    "put",
    "read",
    "reading",
    "reads",
    "real",
    "really",
    "rl:http",
    "said",
    "same",
    "saw",
    "say",
    "says",
    "see",
    "seemed",
    "seen",
    "she",
    "she's",
    "shook",
    "should",
    "show",
    "since",
    "so",
    "some",
    "someone",
    "something",
    "sometimes",
    "soon",
    "sorry",
    "still",
    "such",
    "sure",
    "take",
    "tell",
    "than",
    "that",
    "that's",
    "the",
    "their",
    "them",
    "then",
    "there",
    "these",
    "they",
    "think",
    "this",
    "those",
    "though",
    "thought",
    "through",
    "time",
    "to",
    "together",
    "told",
    "too",
    "took",
    "tried",
    "tries",
    "try",
    "u",
    "under",
    "until",
    "up",
    "us",
    "use",
    "used",
    "using",
    "very",
    "want",
    "wanted",
    "was",
    "wasn't",
    "watched",
    "we",
    "well",
    "were",
    "what",
    "when",
    "where",
    "which",
    "while",
    "who",
    "why",
    "will",
    "with",
    "within",
    "without",
    "work",
    "working",
    "would",
    "www",
    "yet",
    "you",
    "you're",
    "you've",
    "your",
    "yourself",
];
