use crate::index2::tmp_index::TmpWords;
use crate::proc3::stop_words::STOP_WORDS;
use crate::proc3::txt_parse;
#[allow(unused_imports)]
use crate::proc3::txt_parse::{Span, TxtCode, TxtPart};
use html5ever::interface::{ElementFlags, NodeOrText, QuirksMode, TreeSink};
use html5ever::tendril::{StrTendril, TendrilSink};
use html5ever::{parse_document, Attribute, ExpandedName, ParseOpts, QualName};
#[allow(unused_imports)]
use kparse::prelude::TrackProvider;
#[allow(unused_imports)]
use kparse::Track;
use std::borrow::Cow;
use std::fs::File;
use std::io;
use std::io::Write;
use std::time::{Duration, Instant};
use tracking_allocator::AllocationGroupToken;

pub fn timingr<R>(dur: &mut Duration, fun: impl FnOnce() -> R) -> R {
    let now = Instant::now();
    let result = fun();
    *dur += now.elapsed();
    result
}

pub fn index_txt2(
    log: &mut File,
    tok_txt: &mut AllocationGroupToken,
    tok_tmpwords: &mut AllocationGroupToken,
    relative: &str,
    tmp_words: &mut TmpWords,
    text: &str,
) -> Result<usize, io::Error> {
    let mut n_words = 0usize;

    let guard = tok_txt.enter();

    // let tracker = Track::new_tracker::<TxtCode, _>();
    // let mut input = Track::new_span(&tracker, text);
    let mut input = text;
    'l: loop {
        match txt_parse::parse_txt(input) {
            Ok((rest, v)) => {
                input = rest;

                // let r = tracker.results();
                // writeln!(log, "{:#?}", r)?;

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
                        let guard = tok_tmpwords.enter();
                        tmp_words.add_word(word);
                        drop(guard);
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
            }
            Err(e) => {
                println!("{}", relative);
                println!("{:#?}", e);

                writeln!(log, "{}", relative)?;
                writeln!(log, "{:#?}", e)?;

                // let r = tracker.results();
                // writeln!(log, "{:#?}", r)?;

                break 'l;
            }
        }
    }

    drop(guard);

    Ok(n_words)
}

pub fn index_html(
    log: &mut File,
    tok_txt: &mut AllocationGroupToken,
    tok_html: &mut AllocationGroupToken,
    tok_tmpwords: &mut AllocationGroupToken,
    relative: &str,
    words: &mut TmpWords,
    buf: &str,
) -> Result<(), io::Error> {
    #[derive(Debug)]
    struct IdxSink {
        pub txt: String,
        pub elem: Vec<QualName>,
        pub relative: String,
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
            if self.elem.len() > 0 && self.elem.len() % 10000 == 0 {
                println!("html elem={} {}", self.elem.len(), self.relative);
            }
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

    let guard = tok_html.enter();

    let mut s = IdxSink {
        txt: String::with_capacity(buf.len()),
        elem: Vec::default(),
        relative: relative.to_string(),
    };

    let p = parse_document(&mut s, ParseOpts::default());
    p.one(buf);

    drop(guard);

    index_txt2(log, tok_txt, tok_tmpwords, relative, words, s.txt.as_str())?;

    Ok(())
}