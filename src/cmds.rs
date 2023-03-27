use crate::cmdlib::{nom_last_token, nom_ws, CParserResult, CSpan, Cmd, CmdParse};
use kparse::combinators::track;
use kparse::prelude::*;
use kparse::source::SourceStr;
use kparse::{Code, ParserError, Track};
use nom::multi::many1;
use nom::sequence::preceded;
use nom::Parser;
use rustyline::completion::Completer;
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::validate::Validator;
use rustyline::{Context, Helper};
use std::fmt::{Display, Formatter};
use CCode::*;

pub struct Cmds;

impl Helper for Cmds {}

impl Validator for Cmds {}

impl Highlighter for Cmds {}

impl Hinter for Cmds {
    type Hint = String;

    fn hint(&self, line: &str, pos: usize, _ctx: &Context<'_>) -> Option<Self::Hint> {
        let (hint, _, _) = hint_command(self, line, pos);
        hint
    }
}

impl Completer for Cmds {
    type Candidate = String;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Self::Candidate>)> {
        let (_, offset, complete) = hint_command(self, line, pos);
        Ok((offset, complete))
    }
}

fn hint_command(_ctx: &Cmds, line: &str, pos: usize) -> (Option<String>, usize, Vec<String>) {
    let trk = Track::new_tracker::<CCode, _>();
    let span = Track::new_span(&trk, &line[..pos]);
    let txt = Track::source_str(line);

    match parse_cmds(span) {
        Ok((_rest, _cmd)) => hint_none(txt.len()),
        Err(nom::Err::Error(e)) | Err(nom::Err::Failure(e)) => eval_hint_tokens(&txt, e),
        Err(nom::Err::Incomplete(_e)) => hint_none(txt.len()),
    }
}

fn hint_none(len: usize) -> (Option<String>, usize, Vec<String>) {
    (None, len, Vec::new())
}

fn eval_hint_tokens(
    txt: &SourceStr,
    err: ParserError<CCode, CSpan>,
) -> (Option<String>, usize, Vec<String>) {
    let hint = if txt.len() == 0 {
        // don't hint for the empty input
        None
    } else if let Some(sug) = err.iter_expected().next() {
        // trim the hint to remove the prefix already entered.
        let eat = txt.len() - txt.offset(sug.span);

        let token = sug.code.token();
        if eat < token.len() {
            Some(token.split_at(eat).1.to_string())
        } else {
            None
        }
    } else if let Some(sug) = err.iter_suggested().next() {
        // cut already existing text from the suggestion.
        let eat = txt.len() - txt.offset(sug.span);

        let token = sug.code.token();
        if eat < token.len() {
            Some(token.split_at(eat).1.to_string())
        } else {
            None
        }
    } else {
        // cut already existing text from the suggestion.
        let eat = txt.len() - txt.offset(err.span);

        let token = err.code.token();
        if eat < token.len() {
            Some(token.split_at(eat).1.to_string())
        } else {
            None
        }
    };

    // offset for the start of the completions.
    let offset = if let Some(sug) = err.iter_suggested().next() {
        txt.offset(sug.span)
    } else {
        0
    };

    // all possible completions.
    let complete = err
        .iter_suggested()
        .map(|v| v.code.token().to_string())
        .collect();

    (hint, offset, complete)
}

/// ----------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CCode {
    CNomError,

    CCanIgnore,
    CPartMatch,
    CCommand,
    CCommandLoop,

    CBase,
    CDebug,
    CDelete,
    CFiles,
    CFind,
    CHelp,
    CIndex,
    CStats,
    CStore,
    CWhitespace,

    CFindMatch,
    CFilesMatch,
    CDeleteMatch,
}

impl Code for CCode {
    const NOM_ERROR: Self = CNomError;
}

impl Display for CCode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl CCode {
    pub fn token(self) -> &'static str {
        match self {
            CNomError => "",
            CCanIgnore => "",
            CPartMatch => "",
            CCommandLoop => "",

            CWhitespace => "",
            CCommand => "",
            CIndex => "index",
            CFind => "find",
            CHelp => "?",

            CFiles => "files",
            CStats => "stats",
            CDelete => "delete",
            CFindMatch => " <substr>",
            CFilesMatch => " <substr>",
            CDeleteMatch => " <substr>",
            CBase => "base",
            CDebug => "debug",
            CStore => "store",
        }
    }
}

#[derive(Debug, Clone)]
pub enum BCommand {
    Index(),
    Find(Find),
    Files(Files),
    Delete(Delete),
    Stats(Stats),
    Store(),
    Help,
    None,
}

#[derive(Debug, Clone)]
pub enum Delete {
    Delete(String),
}

#[derive(Debug, Clone)]
pub enum Stats {
    Base,
    Debug,
}

#[derive(Debug, Clone)]
pub enum Files {
    Files(String),
}

#[derive(Debug, Clone)]
pub enum Find {
    Find(Vec<String>),
}

pub fn parse_cmds(input: CSpan<'_>) -> CParserResult<'_, BCommand> {
    Track.enter(CCommand, input);
    match ALL_PARSERS.parse(input) {
        Ok((rest, cmd)) => Track.ok(rest, input, cmd),
        Err(e) => Track.err(e),
    }
}

// -----------------------------------------------------------------------
// -----------------------------------------------------------------------

const ALL_PARSERS: CmdParse<BCommand, 9> = CmdParse {
    parse: [
        Cmd::P1("index", CIndex, BCommand::Index()),
        Cmd::P2(
            ("stats", "base"),
            (CStats, CBase),
            BCommand::Stats(Stats::Base),
        ),
        Cmd::P2(
            ("stats", "debug"),
            (CStats, CDebug),
            BCommand::Stats(Stats::Debug),
        ),
        Cmd::P1p("delete", CDelete, parse_delete),
        Cmd::P1p("find", CFind, parse_find),
        Cmd::P1p("files", CFiles, parse_files),
        Cmd::P1("store", CStore, BCommand::Store()),
        Cmd::P1("help", CHelp, BCommand::Help),
        Cmd::P1("?", CHelp, BCommand::Help),
    ],
    fail: BCommand::None,
};

fn parse_delete(input: CSpan<'_>) -> CParserResult<'_, BCommand> {
    track(CDelete, preceded(nom_ws, nom_last_token))
        .map(|v| BCommand::Delete(Delete::Delete(v.fragment().to_string())))
        .with_code(CDeleteMatch)
        .err_into()
        .parse(input)
}

fn parse_files(input: CSpan<'_>) -> CParserResult<'_, BCommand> {
    track(CFiles, preceded(nom_ws, nom_last_token))
        .map(|v| BCommand::Files(Files::Files(v.fragment().to_string())))
        .with_code(CFilesMatch)
        .err_into()
        .parse(input)
}

fn parse_find(input: CSpan<'_>) -> CParserResult<'_, BCommand> {
    track(CFind, many1(preceded(nom_ws, nom_last_token)))
        .map(|spans| {
            BCommand::Find(Find::Find(
                spans
                    .into_iter()
                    .map(|v| v.fragment().to_string())
                    .collect::<Vec<_>>(),
            ))
        })
        .with_code(CFindMatch)
        .err_into()
        .parse(input)
}
