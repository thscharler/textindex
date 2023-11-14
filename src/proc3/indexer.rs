use crate::index2::tmp_index::TmpWords;
use crate::proc3::html_parse2::{HtmlCode, HtmlPart};
use crate::proc3::stop_words::STOP_WORDS;
use crate::proc3::txt_parse::TxtPart;
use crate::proc3::{html_parse2, txt_parse};
#[allow(unused_imports)]
use kparse::prelude::TrackProvider;
#[allow(unused_imports)]
use kparse::spans::SpanFragment;
use kparse::Track;
use std::fs::File;
use std::io;
use std::io::Write;
use std::time::{Duration, Instant};
#[cfg(feature = "allocator")]
use tracking_allocator::AllocationGroupToken;

pub fn timingr<R>(dur: &mut Duration, fun: impl FnOnce() -> R) -> R {
    let now = Instant::now();
    let result = fun();
    *dur += now.elapsed();
    result
}

pub fn index_txt2(
    log: &mut File,
    #[cfg(feature = "allocator")] tok_txt: &mut AllocationGroupToken,
    #[cfg(feature = "allocator")] tok_tmpwords: &mut AllocationGroupToken,
    relative: &str,
    tmp_words: &mut TmpWords,
    text: &str,
) -> Result<usize, io::Error> {
    let mut n_words = 0usize;

    #[cfg(feature = "allocator")]
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
                        #[cfg(feature = "allocator")]
                        let guard = tok_tmpwords.enter();
                        tmp_words.add_word(word);
                        #[cfg(feature = "allocator")]
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

    #[cfg(feature = "allocator")]
    drop(guard);

    Ok(n_words)
}

pub fn index_html2(
    log: &mut File,
    #[cfg(feature = "allocator")] tok_txt: &mut AllocationGroupToken,
    #[cfg(feature = "allocator")] tok_html: &mut AllocationGroupToken,
    #[cfg(feature = "allocator")] tok_tmpwords: &mut AllocationGroupToken,
    relative: &str,
    words: &mut TmpWords,
    text: &str,
) -> Result<(), io::Error> {
    #[cfg(feature = "allocator")]
    let guard = tok_html.enter();

    let mut buf = String::with_capacity(text.len());

    let tracker = Track::new_tracker::<HtmlCode, _>();
    let mut input = Track::new_span(&tracker, text);
    // let mut input = text;
    'l: loop {
        match html_parse2::parse_html(input) {
            Ok((rest, v)) => {
                input = rest;

                // let r = tracker.results();
                // writeln!(log, "{:#?}", r)?;

                match v {
                    HtmlPart::Text(v) => {
                        buf.push_str(*v.fragment());
                    }
                    HtmlPart::StartTag(_)
                    | HtmlPart::EndTag(_)
                    | HtmlPart::DocType(_)
                    | HtmlPart::Comment(_)
                    | HtmlPart::CData(_) => {
                        // ignore
                    }
                    HtmlPart::CharRef(v) => {
                        buf.push(v);
                    }
                    HtmlPart::CharRefStr(v) => {
                        buf.push_str(v);
                    }
                    HtmlPart::Eof => {
                        break 'l;
                    }
                    HtmlPart::ParseError(_) => {}
                }
            }
            Err(e) => {
                println!("{}", relative);
                println!("{:#?}", e);

                writeln!(log, "{}", relative)?;
                writeln!(log, "{:#?}", e)?;

                let r = tracker.results();
                // println!("{:#?}", r);
                writeln!(log, "{:#?}", r)?;

                break 'l;
            }
        }
    }

    index_txt2(
        log,
        #[cfg(feature = "allocator")]
        tok_txt,
        #[cfg(feature = "allocator")]
        tok_tmpwords,
        relative,
        words,
        buf.as_str(),
    )?;

    Ok(())
}
