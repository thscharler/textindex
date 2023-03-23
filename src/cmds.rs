use crate::cmdlib::{
    nom_empty, nom_last_token, nom_ws, CParserError, CParserResult, CSpan, ParseCmd, ParseCmd2,
    SubCmd,
};
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
    let hint = if let Some(sug) = err.iter_suggested().next() {
        // nur den rest des vorschlags verwenden.
        let eat = txt.len() - txt.offset(sug.span);

        let token = sug.code.token();
        if eat < token.len() {
            Some(token.split_at(eat).1.to_string())
        } else {
            None
        }
    } else {
        // nur den rest des vorschlags verwenden.
        let eat = txt.len() - txt.offset(err.span);

        let token = err.code.token();
        if eat < token.len() {
            Some(token.split_at(eat).1.to_string())
        } else {
            None
        }
    };

    let offset = if let Some(sug) = err.iter_suggested().next() {
        txt.offset(sug.span)
    } else {
        0
    };

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
    CIgnore,
    CWhitespace,

    CCommand,
    CIndex,
    CFind,
    CHelp,
    CFiles,
    CStats,
    CBase,
    CDebug,
    CDelete,
    CStore,
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
            CIgnore => "",
            CWhitespace => "",
            CCommand => "",
            CIndex => "index",
            CFind => "find",
            CHelp => "?",

            CFiles => "files",
            CStats => "stats",
            CDelete => "delete",
            CFindMatch => "<substr>",
            CFilesMatch => "<substr>",
            CDeleteMatch => "<substr>",
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
    Help(),
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

fn parse_loop(input: CSpan<'_>) -> CParserResult<'_, BCommand> {
    let mut err: Option<CParserError<'_>> = None;

    match PARSE_INDEX.parse(input) {
        Ok(v) => return Ok(v),
        Err(e) => {
            err.append(e);
        }
    }
    match PARSE_FIND.parse(input) {
        Ok(v) => return Ok(v),
        Err(e) => {
            err.append(e);
        }
    };
    match PARSE_FILES.parse(input) {
        Ok(v) => return Ok(v),
        Err(e) => {
            err.append(e);
        }
    };
    match PARSE_DELETE.parse(input) {
        Ok(v) => return Ok(v),
        Err(e) => {
            err.append(e);
        }
    };
    match PARSE_STATS.parse(input) {
        Ok(v) => return Ok(v),
        Err(e) => {
            err.append(e);
        }
    };
    match PARSE_STORE.parse(input) {
        Ok(v) => return Ok(v),
        Err(e) => {
            err.append(e);
        }
    };
    match PARSE_HELP_1.parse(input) {
        Ok(v) => return Ok(v),
        Err(e) => {
            err.append(e);
        }
    };
    match PARSE_HELP_2.parse(input) {
        Ok(v) => return Ok(v),
        Err(e) => {
            err.append(e);
        }
    };

    match err {
        Some(err) => Err(nom::Err::Error(err)),
        None => Err(nom::Err::Error(CParserError::new(CCommand, input))),
    }
}

pub fn parse_cmds(input: CSpan<'_>) -> CParserResult<'_, BCommand> {
    Track.enter(CCommand, input);
    match parse_loop(input) {
        Ok((rest, cmd)) => Track.ok(rest, input, cmd),
        Err(nom::Err::Error(err)) => {
            if !input.is_empty() {
                Track.err(err)
            } else {
                Track.ok(input, nom_empty(input), BCommand::None)
            }
        }
        Err(e) => Track.err(e),
    }
}

const PARSE_INDEX: ParseCmd<(), BCommand> = ParseCmd {
    to_cmd: |_| BCommand::Index(),
    sub: SubCmd {
        token: "index",
        code: CIndex,
        to_out: |v| Ok((v, ())),
    },
};

const PARSE_STATS: ParseCmd2<Stats, BCommand, 2> = ParseCmd2 {
    to_cmd: BCommand::Stats,
    token: "stats",
    code: CStats,
    list: [
        SubCmd {
            token: "base",
            code: CBase,
            to_out: |v| Ok((v, Stats::Base)),
        },
        SubCmd {
            token: "debug",
            code: CDebug,
            to_out: |v| Ok((v, Stats::Debug)),
        },
    ],
};

const PARSE_DELETE: ParseCmd<Delete, BCommand> = ParseCmd {
    to_cmd: BCommand::Delete,
    sub: SubCmd {
        token: "delete",
        code: CDelete,
        to_out: parse_delete,
    },
};

const PARSE_FIND: ParseCmd<Find, BCommand> = ParseCmd {
    to_cmd: BCommand::Find,
    sub: SubCmd {
        token: "find",
        code: CFind,
        to_out: parse_find,
    },
};

const PARSE_FILES: ParseCmd<Files, BCommand> = ParseCmd {
    to_cmd: BCommand::Files,
    sub: SubCmd {
        token: "files",
        code: CFiles,
        to_out: parse_files,
    },
};

const PARSE_STORE: ParseCmd<(), BCommand> = ParseCmd {
    to_cmd: |_| BCommand::Store(),
    sub: SubCmd {
        token: "store",
        code: CStore,
        to_out: |v| Ok((v, ())),
    },
};

const PARSE_HELP_1: ParseCmd<(), BCommand> = ParseCmd {
    to_cmd: |_| BCommand::Help(),
    sub: SubCmd {
        token: "help",
        code: CHelp,
        to_out: |v| Ok((v, ())),
    },
};

const PARSE_HELP_2: ParseCmd<(), BCommand> = ParseCmd {
    to_cmd: |_| BCommand::Help(),
    sub: SubCmd {
        token: "?",
        code: CHelp,
        to_out: |v| Ok((v, ())),
    },
};

fn parse_find(input: CSpan<'_>) -> CParserResult<'_, Find> {
    track(CFind, many1(preceded(nom_ws, nom_last_token)))
        .map(|spans| {
            Find::Find(
                spans
                    .into_iter()
                    .map(|v| v.fragment().to_string())
                    .collect::<Vec<_>>(),
            )
        })
        .with_code(CFindMatch)
        .err_into()
        .parse(input)
}

fn parse_files(input: CSpan<'_>) -> CParserResult<'_, Files> {
    track(CFiles, preceded(nom_ws, nom_last_token))
        .map(|v| Files::Files(v.fragment().to_string()))
        .with_code(CFilesMatch)
        .err_into()
        .parse(input)
}

fn parse_delete(input: CSpan<'_>) -> CParserResult<'_, Delete> {
    track(CDelete, preceded(nom_ws, nom_last_token))
        .map(|v| Delete::Delete(v.fragment().to_string()))
        .with_code(CDeleteMatch)
        .err_into()
        .parse(input)
}
