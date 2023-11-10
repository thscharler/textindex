use crate::index2::tmp_index::TmpWords;
use crate::proc3::stop_words::STOP_WORDS;
use crate::proc3::txt_parse;
use crate::proc3::txt_parse::{TxtCode, TxtPart};
use html5ever::interface::{ElementFlags, NodeOrText, QuirksMode, TreeSink};
use html5ever::tendril::{StrTendril, TendrilSink};
use html5ever::{parse_document, Attribute, ExpandedName, ParseOpts, QualName};
use kparse::prelude::TrackProvider;
use kparse::Track;
use std::borrow::Cow;
use std::fs::File;
use std::io;
use std::io::Write;
use std::time::{Duration, Instant};

pub fn timingr<R>(dur: &mut Duration, fun: impl FnOnce() -> R) -> R {
    let now = Instant::now();
    let result = fun();
    *dur += now.elapsed();
    result
}

pub fn index_txt2(
    log: &mut File,
    relative: &str,
    tmp_words: &mut TmpWords,
    text: &str,
) -> Result<usize, io::Error> {
    let mut n_words = 0usize;

    let tracker = Track::new_tracker::<TxtCode, _>();
    let mut input = Track::new_span(&tracker, text);
    // let mut input = text;
    'l: loop {
        match txt_parse::parse_txt(input) {
            Ok((rest, v)) => {
                match v {
                    TxtPart::Text(v) => {
                        n_words += 1;
                        let word = v.to_lowercase();
                        if STOP_WORDS
                            .binary_search_by(|probe| (*probe).cmp(word.as_ref()))
                            .is_ok()
                        {
                            continue 'l;
                        }
                        // spurios tags
                        // if word.contains('<') || word.contains(">") {
                        //     continue 'l;
                        // }
                        tmp_words.add_word(word);
                    }
                    TxtPart::Eof => {
                        break 'l;
                    }
                    TxtPart::Tag => {}
                    TxtPart::Pgp => {}
                    TxtPart::Base64 => {}
                    TxtPart::KeyValue => {}
                    TxtPart::NonText => {}
                    TxtPart::NewLine => {}
                }
                // let r = tracker.results();
                // writeln!(log, "{:#?}", r)?;

                input = rest;
            }
            Err(e) => {
                let r = tracker.results();
                println!("{}", relative);
                println!("{:#?}", e);
                println!("{:#?}", r);
                writeln!(log, "{}", relative)?;
                writeln!(log, "{:#?}", e)?;
                writeln!(log, "{:#?}", r)?;
            }
        }
    }

    Ok(n_words)
}

pub fn index_txt(
    _log: &mut File,
    _relative: &str,
    tmp_words: &mut TmpWords,
    text: &str,
) -> Result<usize, io::Error> {
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
        if base64_section && line.starts_with('M') {
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
            let mut it = word.chars();
            if let Some(c) = it.next() {
                // numeric data ignored
                if c.is_numeric() {
                    continue;
                }
                if let Some(c) = it.next() {
                    match c {
                        '+' | '=' | '!' | '"' | '#' | '$' | '%' | '&' | '(' | ')' | '[' | ']'
                        | '*' | '0' | '1' | '2' | '3' | '4' | '5' | '6' | '7' | '8' | '9' | ':'
                        | ';' | '?' | '@' | '\\' | '~' | '`' => {
                            continue;
                        }
                        _ => {}
                    }
                }
            }
            if STOP_WORDS
                .binary_search_by(|probe| (*probe).cmp(word))
                .is_ok()
            {
                continue;
            }
            // spurios tags
            if word.contains('<') || word.contains(">") {
                continue;
            }
            if word.is_empty() {
                continue;
            }

            n_words += 1;
            tmp_words.add_word(word.to_lowercase());
        }
    }

    Ok(n_words)
}

fn trim_word(word: &str) -> &str {
    word.trim_matches(|c: char| !c.is_alphanumeric())
}

pub fn index_html(
    log: &mut File,
    relative: &str,
    words: &mut TmpWords,
    buf: &str,
) -> Result<(), io::Error> {
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

    index_txt(log, relative, words, s.txt.as_str())?;

    Ok(())
}
