use crate::cmdlib::{CParserError, CSpan};
use crate::cmds::CCode;
use kparse::parser_error::SpanAndCode;
use kparse::prelude::*;
use kparse::provider::TrackedDataVec;
use kparse::Track;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;

#[allow(dead_code)]
pub fn log_input(line: &str, pos: usize) {
    let log = PathBuf::from("input.log");
    if !log.exists() {
        let _ = File::create(&log);
    }
    if let Ok(mut f) = OpenOptions::new().append(true).open(log) {
        let _ = writeln!(f, "{}\t{}", line, pos);
    };
}

#[allow(dead_code)]
pub fn log_trace(trace: &TrackedDataVec<CCode, &str>) {
    let log = PathBuf::from("input.log");
    if !log.exists() {
        let _ = File::create(&log);
    }
    if let Ok(mut f) = OpenOptions::new().append(true).open(log) {
        let _ = writeln!(f, "{:?}", trace);
    };
}

pub fn dump_diagnostics(str: &str, err: &CParserError<'_>, msg: &str, is_err: bool) {
    let txt = Track::source_str(str);

    println!();
    if !msg.is_empty() {
        println!("{}: {:?}", if is_err { "FEHLER" } else { "WARNUNG" }, msg);
    } else {
        println!(
            "{}: {:?} ",
            if is_err { "FEHLER" } else { "WARNUNG" },
            err.code,
        );
    }

    println!("{}", str);

    println!("{}^", " ".repeat(txt.column(err.span)));
    if !msg.is_empty() {
        println!("Erwarted war: {}", msg);
    } else {
        println!("Erwarted war: '{:?}'", err.code);
    }

    let ex = dedup_spans(err.code, err.iter_expected());
    for exp in ex {
        println!("{}^", " ".repeat(txt.column(err.span)));
        println!("Erwarted war: '{:?}'", exp.code);
    }

    let sg = dedup_spans(err.code, err.iter_suggested());
    for sug in sg {
        println!("Hinweis: '{:?}'", sug.code);
    }
}

fn dedup_spans<'a>(
    mc: CCode,
    it: impl Iterator<Item = SpanAndCode<CCode, CSpan<'a>>>,
) -> Vec<SpanAndCode<CCode, CSpan<'a>>> {
    let mut c = it.filter(|v| v.code != mc).collect::<Vec<_>>();
    c.dedup_by(|v, w| v.code == w.code);
    c
}
