use crate::cmdlib::{
    nom_empty, nom_ws, nom_ws_span, CParserResult, CSpan, Parse1LayerCommand, Parse1Layers,
    Parse2LayerCommand, Parse2Layers, SubCmd,
};
use kparse::prelude::*;
use kparse::source::SourceStr;
use kparse::{define_span, Code, ParserError, TokenizerResult, Track};
use rustyline::completion::Completer;
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::validate::Validator;
use rustyline::{Context, Helper};
use std::fmt::{Display, Formatter};
use CCode::*;

pub struct Cmds {}

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
        Ok((_, _)) => hint_none(txt.len()),
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
    CText,
    CHelp,

    CFileName,
}

impl Code for CCode {
    const NOM_ERROR: Self = CNomError;
}

impl Display for CCode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            CNomError => "NomError",
            CIgnore => "Ignore",

            CCommand => "Command",
            CIndex => "Index",
            CFind => "Find",
            CText => "Text",

            CWhitespace => "Whitespace1",
            CFileName => "FileName",
            CHelp => "Help",
        };
        write!(f, "{}", name)
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
            CFileName => "",
            CText => "text",
            CHelp => "?",
        }
    }
}

#[derive(Debug, Clone)]
pub enum BCommand {
    Index(Index),
    Find(Find),
    Help(Help),
    None,
}

#[derive(Debug, Clone, Copy)]
pub enum Index {
    Index,
}

#[derive(Debug, Clone)]
pub enum Find {
    Find(String),
}

#[derive(Debug, Clone)]
pub enum Help {
    Help,
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

const PARSE_INDEX: Parse1LayerCommand = Parse1LayerCommand {
    cmd: BCommand::Index(Index::Index),
    layers: Parse1Layers {
        token: "index",
        code: CIndex,
    },
};

const PARSE_FIND: Parse2LayerCommand<Find, 1> = Parse2LayerCommand {
    layers: Parse2Layers {
        token: "find",
        code: CFind,
        list: [SubCmd {
            token: "text",
            code: CText,
            output: parse_text,
        }],
    },
    map_cmd: BCommand::Find,
};

const PARSE_HELP_1: Parse1LayerCommand = Parse1LayerCommand {
    cmd: BCommand::Help(Help::Help),
    layers: Parse1Layers {
        token: "help",
        code: CHelp,
    },
};

const PARSE_HELP_2: Parse1LayerCommand = Parse1LayerCommand {
    cmd: BCommand::Help(Help::Help),
    layers: Parse1Layers {
        token: "?",
        code: CHelp,
    },
};

fn parse_text(input: CSpan<'_>) -> CParserResult<'_, Find> {
    Track.enter(CFind, input);
    let (rest, ws) = nom_ws(input).err_into().track()?;
    if !ws.is_empty() {
        let tok = input.fragment();
        Track.ok(rest, input, Find::Find(tok.to_string()))
    } else {
        Track.ok(rest, input, Find::Find("".to_string()))
    }
}
