use html5ever::interface::{ElementFlags, NodeOrText, QuirksMode, TreeSink};
use html5ever::tendril::{StrTendril, TendrilSink};
use html5ever::{parse_document, Attribute, ExpandedName, ParseOpts, QualName};
use std::borrow::Cow;
use std::collections::BTreeSet;
use std::fmt::Debug;
use std::time::{Duration, Instant};

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

pub fn timingr<R>(dur: &mut Duration, fun: impl FnOnce() -> R) -> R {
    let now = Instant::now();
    let result = fun();
    *dur += now.elapsed();
    result
}

pub fn index_txt(tmp_words: &mut TmpWords, text: &str) -> usize {
    let mut n_words = 0usize;

    let mut base64_section = false;
    let mut pgp_section = false;

    for line in text.split('\n') {
        // skip headers like:
        // Subject: some subject ...
        if let Some(header) = line.split_once(|c: char| c == ':') {
            if header
                .0
                .find(|c: char| !c.is_ascii_alphanumeric() && c != '-' && c != '*')
                .is_none()
            {
                continue;
            }
        }
        if let Some(c) = line.chars().next() {
            // text-frames ignored
            if c == '|' || c == '+' {
                continue;
            }
        }
        if line.starts_with("begin") {
            base64_section = true;
        }
        if base64_section && line.starts_with("end") {
            base64_section = false;
        }
        if base64_section && line.starts_with("M") {
            continue;
        }
        if line.contains("-----BEGIN PGP SIGNATURE-----") {
            pgp_section = true;
        }
        if pgp_section && line.contains("-----END PGP SIGNATURE-----") {
            pgp_section = false;
        }
        if pgp_section {
            continue;
        }

        // split at white
        let words = line.split(|c: char| {
            c as u32 <= 32
                || c == '_'
                || c == ','
                || c == '.'
                || c == '-'
                || c == '\u{FFFD}'
                || c.is_whitespace()
        });
        for word in words {
            let word = trim_word(word);
            if let Some(c) = word.chars().next() {
                // numeric data ignored
                if c.is_numeric() {
                    continue;
                }
            }
            if word.is_empty() {
                continue;
            }

            n_words += 1;
            tmp_words.add_word(word.to_lowercase());
        }
    }

    n_words
}

fn trim_word(word: &str) -> &str {
    word.trim_matches(|c: char| !c.is_alphanumeric())
}

pub fn index_html(words: &mut TmpWords, buf: &str) {
    #[derive(Debug)]
    struct IdxSink {
        pub txt: String,
        pub elem: Vec<QualName>,
        // pub comment: Vec<StrTendril>,
        // pub pi: Vec<(StrTendril, StrTendril)>,
    }

    #[derive(Clone, Debug)]
    enum IdxHandle {
        Elem(usize),
        Comment(usize),
        Pi(usize),
    }

    impl TreeSink for &mut IdxSink {
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
                IdxHandle::Comment(_) => unimplemented!(),
                IdxHandle::Pi(_) => unimplemented!(),
            }
        }

        fn create_element(
            &mut self,
            name: QualName,
            _attrs: Vec<Attribute>,
            _flags: ElementFlags,
        ) -> Self::Handle {
            let handle = self.elem.len();
            self.elem.push(name);
            IdxHandle::Elem(handle)
        }

        fn create_comment(&mut self, _text: StrTendril) -> Self::Handle {
            // no need to store, always hand out 0
            // let handle = self.comment.len();
            // self.comment.push(text);
            IdxHandle::Comment(0)
        }

        fn create_pi(&mut self, _target: StrTendril, _data: StrTendril) -> Self::Handle {
            // no need to store, always hand out 0
            // let handle = self.pi.len();
            // self.pi.push((target, data));
            IdxHandle::Pi(0)
        }

        fn append(&mut self, _parent: &Self::Handle, child: NodeOrText<Self::Handle>) {
            match child {
                NodeOrText::AppendNode(_n) => {}
                NodeOrText::AppendText(v) => {
                    self.txt.push_str(v.as_ref());
                }
            }
        }

        fn append_based_on_parent_node(
            &mut self,
            _element: &Self::Handle,
            _prev_element: &Self::Handle,
            _child: NodeOrText<Self::Handle>,
        ) {
        }

        fn append_doctype_to_document(
            &mut self,
            _name: StrTendril,
            _public_id: StrTendril,
            _system_id: StrTendril,
        ) {
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
        }

        fn add_attrs_if_missing(&mut self, _target: &Self::Handle, _attrs: Vec<Attribute>) {}

        fn remove_from_parent(&mut self, _target: &Self::Handle) {}

        fn reparent_children(&mut self, _node: &Self::Handle, _new_parent: &Self::Handle) {}
    }

    let mut s = IdxSink {
        txt: String::default(),
        elem: Vec::default(),
    };

    let p = parse_document(&mut s, ParseOpts::default());
    p.one(buf);

    index_txt(words, s.txt.as_str());
}

pub const STOP_WORDS: &[&str] = &[
    "a",
    "about",
    "after",
    "again",
    "against",
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
    "approved",
    "archive",
    "are",
    "around",
    "as",
    "asked",
    "assm",
    "at",
    "author",
    "away",
    "back",
    "be",
    "bearded",
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
    "contact",
    "could",
    "couldn't",
    "date",
    "day",
    "did",
    "didn't",
    "do",
    "don't",
    "done",
    "down",
    "each",
    "eli",
    "else",
    "end",
    "even",
    "ever",
    "every",
    "faq",
    "few",
    "finally",
    "find",
    "finds",
    "first",
    "for",
    "found",
    "from",
    "get",
    "gets",
    "give",
    "gmt",
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
    "html",
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
    "let",
    "like",
    "little",
    "little-neck",
    "long",
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
    "moderated",
    "moderator",
    "more",
    "most",
    "much",
    "must",
    "my",
    "myself",
    "need",
    "net",
    "net",
    "never",
    "new",
    "news1",
    "newsgroup",
    "newsgroups",
    "next",
    "no",
    "not",
    "notes",
    "nothin",
    "now",
    "ny",
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
    "path",
    "please",
    "put",
    "read",
    "reading",
    "reads",
    "real",
    "really",
    "right",
    "runscroller",
    "s",
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
    "spam",
    "started",
    "still",
    "story",
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
    "thwart",
    "time",
    "to",
    "together",
    "told",
    "too",
    "took",
    "tried",
    "tries",
    "try",
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
