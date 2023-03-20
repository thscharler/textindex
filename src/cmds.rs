use crate::cmdlib::{
    nom_empty, nom_last_token, nom_ws, nom_ws_span, CParserResult, CSpan, ParseCmd, ParseCmd2,
    SubCmd,
};
use kparse::combinators::track;
use kparse::prelude::*;
use kparse::source::SourceStr;
use kparse::{Code, ParserError, Track};
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
    CDelete,
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
}

#[derive(Debug, Clone)]
pub enum Files {
    Files(String),
}

#[derive(Debug, Clone)]
pub enum Find {
    Find(String),
}

pub fn parse_cmds(rest: CSpan<'_>) -> CParserResult<'_, BCommand> {
    Track.enter(CCommand, rest);

    let mut command = None;
    let mut err = None;

    if PARSE_INDEX.lah(rest) {
        match PARSE_INDEX.parse(rest) {
            Ok((_, cmd)) => command = Some(cmd),
            Err(e) => err.append(e)?,
        }
    }
    if PARSE_FIND.lah(rest) {
        match PARSE_FIND.parse(rest) {
            Ok((_, cmd)) => command = Some(cmd),
            Err(e) => err.append(e)?,
        }
    }
    if PARSE_FILES.lah(rest) {
        match PARSE_FILES.parse(rest) {
            Ok((_, cmd)) => command = Some(cmd),
            Err(e) => err.append(e)?,
        }
    }
    if PARSE_DELETE.lah(rest) {
        match PARSE_DELETE.parse(rest) {
            Ok((_, cmd)) => command = Some(cmd),
            Err(e) => err.append(e)?,
        }
    }
    if PARSE_STATS.lah(rest) {
        match PARSE_STATS.parse(rest) {
            Ok((_, cmd)) => command = Some(cmd),
            Err(e) => err.append(e)?,
        }
    }
    if PARSE_HELP_1.lah(rest) {
        match PARSE_HELP_1.parse(rest) {
            Ok((_, cmd)) => command = Some(cmd),
            Err(e) => err.append(e)?,
        }
    }
    if PARSE_HELP_2.lah(rest) {
        match PARSE_HELP_2.parse(rest) {
            Ok((_, cmd)) => command = Some(cmd),
            Err(e) => err.append(e)?,
        }
    }

    if let Some(command) = command {
        Track.ok(rest, nom_empty(rest), command)
    } else {
        let rest = nom_ws_span(rest);
        if !rest.is_empty() {
            if let Some(err) = err {
                Track.err(err)
            } else {
                Track.err(ParserError::new(CCommand, rest))
            }
        } else {
            Track.ok(rest, nom_empty(rest), BCommand::None)
        }
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

const PARSE_STATS: ParseCmd2<Stats, BCommand, 1> = ParseCmd2 {
    to_cmd: BCommand::Stats,
    token: "stats",
    code: CStats,
    list: [SubCmd {
        token: "base",
        code: CBase,
        to_out: |v| Ok((v, Stats::Base)),
    }],
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
    track(CFind, preceded(nom_ws, nom_last_token))
        .map(|v| Find::Find(v.fragment().to_string()))
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
