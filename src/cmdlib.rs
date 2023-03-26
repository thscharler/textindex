#![allow(dead_code)]

use crate::cmds::CCode;
use crate::cmds::CCode::*;
use kparse::prelude::*;
use kparse::{ParserError, ParserResult, TokenizerError, TokenizerResult};
use nom::bytes::complete::{tag, take_till1, take_while1};
use nom::combinator::recognize;
use nom::InputTake;
use nom::{AsChar, InputTakeAtPosition};

define_span!(pub CSpan = CCode, str);
pub type CParserResult<'s, O> = ParserResult<CCode, CSpan<'s>, O>;
pub type CTokenizerResult<'s, O> = TokenizerResult<CCode, CSpan<'s>, O>;
pub type CParserError<'s> = ParserError<CCode, CSpan<'s>>;
pub type CTokenizerError<'s> = TokenizerError<CCode, CSpan<'s>>;

// -----------------------------------------------------------------------
// -----------------------------------------------------------------------

pub struct CmdParse<T, const N: usize> {
    pub parse: [Cmd<T>; N],
    pub fail: T,
}

pub type PFn<T> = fn(CSpan<'_>) -> CParserResult<'_, T>;

pub enum Cmd<T> {
    P1(&'static str, CCode, T),
    P2((&'static str, &'static str), (CCode, CCode), T),
    P1p(&'static str, CCode, PFn<T>),
    P2p((&'static str, &'static str), (CCode, CCode), PFn<T>),
}

// -----------------------------------------------------------------------
// -----------------------------------------------------------------------

impl<T, const N: usize> CmdParse<T, N>
where
    T: Clone,
{
    pub fn parse<'s>(&self, input: CSpan<'s>) -> CParserResult<'s, T> {
        Track.enter(CCommandLoop, input);

        let (rest, _) = nom_ws(input).err_into().track()?;

        let mut partial = None;
        let mut err: Option<CParserError<'_>> = None;
        for cmd in &self.parse {
            match cmd.parse(rest) {
                Ok((rest, v)) => {
                    return Track.ok(rest, input, v);
                }
                Err(nom::Err::Error(e)) if e.is_suggested(CPartMatch) => {
                    // there should be just on partial match at most.
                    partial = Some(e);
                }
                Err(nom::Err::Error(e)) => {
                    // ignore if there is not even a prefix match
                    if e.code != CCanIgnore {
                        err.append(e);
                    }
                }
                Err(e) => {
                    return Track.err(e);
                }
            }
        }

        match (err, partial) {
            (Some(err), _) => {
                return Track.err(err);
            }
            (None, Some(p)) => {
                // collect alternatives with the same code_1
                let mut err = ParserError::new(p.code, p.span);
                for cmd in &self.parse {
                    let sug_code = match cmd {
                        Cmd::P2(_, (t, c), _) if *t == p.code => *c,
                        Cmd::P2p(_, (t, c), _) if *t == p.code => *c,
                        _ => CCanIgnore,
                    };
                    if sug_code != CCanIgnore {
                        err.suggest(sug_code, p.span);
                    }
                }
                return Track.err(err);
            }
            (None, _) => {
                // not even one prefix match. list all.
                let mut err = ParserError::new(CCommand, input);
                for cmd in &self.parse {
                    let sug_code = match cmd {
                        Cmd::P1(_, c, _) => *c,
                        Cmd::P2(_, (c, _), _) => *c,
                        Cmd::P1p(_, c, _) => *c,
                        Cmd::P2p(_, (c, _), _) => *c,
                    };
                    if !err.is_suggested(sug_code) {
                        err.suggest(sug_code, input);
                    }
                }
                return Track.err(err);
            }
        }
    }
}

impl<T> Cmd<T>
where
    T: Clone,
{
    fn parse_p1<'s>(
        input: CSpan<'s>,
        tok1: &str,
        code1: CCode,
        result: &T,
    ) -> CParserResult<'s, T> {
        Track.enter(code1, input);

        match token_command(tok1, code1, input) {
            Ok((rest, _v)) => {
                consumed_all(rest, code1)?;
                return Track.ok(rest, input, result.clone());
            }
            Err(nom::Err::Error(e)) if e.code == CCanIgnore => {
                return Track.err(e);
            }
            Err(e) => {
                return Track.err(e.with_code(code1));
            }
        }
    }

    fn parse_p1p<'s>(
        input: CSpan<'s>,
        tok1: &str,
        code1: CCode,
        result_fn: PFn<T>,
    ) -> CParserResult<'s, T> {
        Track.enter(code1, input);

        match token_command(tok1, code1, input) {
            Ok((rest, _)) => {
                consumed_all(rest, code1).track()?;
                let (_, v) = result_fn(rest).track()?;
                return Track.ok(rest, input, v);
            }
            Err(nom::Err::Error(e)) if e.code == CCanIgnore => {
                return Track.err(e);
            }
            Err(e) => {
                return Track.err(e.with_code(code1));
            }
        }
    }

    fn parse_p2<'s>(
        input: CSpan<'s>,
        tok1: &str,
        tok2: &str,
        code1: CCode,
        code2: CCode,
        result: &T,
    ) -> CParserResult<'s, T> {
        Track.enter(code1, input);

        match token_command(tok1, code1, input) {
            Ok((rest, _)) => {
                let (rest, _) = nom_ws1(rest).err_into().track()?;
                let (rest, v) = Self::parse_p2_cont(rest, tok2, code1, code2, result).track()?;
                return Track.ok(rest, input, v);
            }
            Err(nom::Err::Error(e)) if e.code == CCanIgnore => {
                return Track.err(e);
            }
            Err(e) => {
                return Track.err(e.with_code(code1));
            }
        }
    }

    fn parse_p2_cont<'s>(
        input: CSpan<'s>,
        tok2: &str,
        code1: CCode,
        code2: CCode,
        result: &T,
    ) -> CParserResult<'s, T> {
        Track.enter(code2, input);

        match token_command(tok2, code2, input) {
            Ok((rest, _v)) => {
                consumed_all(rest, code2).track()?;
                return Track.ok(rest, input, result.clone());
            }
            Err(nom::Err::Error(e)) if e.code == CCanIgnore => {
                let mut err = ParserError::new(code1, e.span);
                err.suggest(CPartMatch, e.span);
                return Track.err(nom::Err::Error(err));
            }
            Err(e) => {
                return Track.err(e.with_code(code2).with_code(code1));
            }
        }
    }

    fn parse_p2p<'s>(
        input: CSpan<'s>,
        tok1: &str,
        tok2: &str,
        code1: CCode,
        code2: CCode,
        result_fn: PFn<T>,
    ) -> CParserResult<'s, T> {
        Track.enter(code1, input);

        match token_command(tok1, code1, input) {
            Ok((rest, _)) => {
                let (rest, _) = nom_ws1(rest).err_into().track()?;
                let (rest, v) =
                    Self::parse_p2p_cont(rest, tok2, code1, code2, result_fn).track()?;
                return Track.ok(rest, input, v);
            }
            Err(nom::Err::Error(e)) if e.code == CCanIgnore => {
                return Track.err(e);
            }
            Err(e) => {
                return Track.err(e.with_code(code1));
            }
        }
    }

    fn parse_p2p_cont<'s>(
        input: CSpan<'s>,
        tok2: &str,
        code1: CCode,
        code2: CCode,
        result_fn: PFn<T>,
    ) -> CParserResult<'s, T> {
        Track.enter(code2, input);

        match token_command(tok2, code2, input) {
            Ok((rest, _v)) => {
                let (rest, v) = result_fn(rest).track()?;
                consumed_all(rest, code2).track()?;
                return Track.ok(rest, input, v);
            }
            Err(nom::Err::Error(e)) if e.code == CCanIgnore => {
                let mut err = ParserError::new(code1, e.span);
                err.suggest(CPartMatch, e.span);
                return Track.err(nom::Err::Error(err));
            }
            Err(e) => {
                return Track.err(e.with_code(code2).with_code(code1));
            }
        }
    }

    pub fn parse<'s>(&self, input: CSpan<'s>) -> CParserResult<'s, T> {
        match self {
            Cmd::P1(tok, code, res) => {
                return Self::parse_p1(input, tok, *code, res);
            }
            Cmd::P2(tok, code, res) => {
                return Self::parse_p2(input, tok.0, tok.1, code.0, code.1, res);
            }
            Cmd::P1p(tok, code, res) => {
                return Self::parse_p1p(input, tok, *code, *res);
            }
            Cmd::P2p(tok, code, res) => {
                return Self::parse_p2p(input, tok.0, tok.1, code.0, code.1, *res);
            }
        }
    }
}

// -----------------------------------------------------------------------
// -----------------------------------------------------------------------

/// Tries to parse the token. If it fails and at least partially matches it adds a Suggest.
fn token_command<'a>(tok: &'_ str, code: CCode, rest: CSpan<'a>) -> CParserResult<'a, CSpan<'a>> {
    let (rest, token) = match tag::<_, _, CParserError<'a>>(tok)(rest) {
        Ok((rest, token)) => (rest, token),
        Err(nom::Err::Error(_) | nom::Err::Failure(_)) => {
            //
            match nom_last_token(rest) {
                Ok((_, last)) => {
                    let err = if tok.starts_with(&last.to_lowercase()) {
                        let mut err = CParserError::new(code, last);
                        err.suggest(code, last);
                        err
                    } else {
                        CParserError::new(CCanIgnore, rest)
                    };
                    return Err(nom::Err::Error(err));
                }
                Err(_) => return Err(nom::Err::Error(CParserError::new(CCanIgnore, rest))),
            }
        }
        Err(nom::Err::Incomplete(_)) => unreachable!(),
    };

    Ok((rest, token))
}

/// Returns a token, but only if it ends the line.
pub fn nom_last_token(i: CSpan<'_>) -> CTokenizerResult<'_, CSpan<'_>> {
    match recognize::<_, _, CTokenizerError<'_>, _>(take_till1(|c: char| c == ' ' || c == '\t'))(i)
    {
        Ok((rest, tok)) => Ok((rest, tok)),
        _ => Err(nom::Err::Error(CTokenizerError::new(CNomError, i))),
    }
}

pub fn nom_empty(i: CSpan<'_>) -> CSpan<'_> {
    i.take(0)
}

/// Eat whitespace
pub fn nom_ws_span(i: CSpan<'_>) -> CSpan<'_> {
    match i.split_at_position_complete::<_, nom::error::Error<CSpan<'_>>>(|item| {
        let c = item.as_char();
        !(c == ' ' || c == '\t')
    }) {
        Ok((rest, _)) => rest,
        Err(_) => i,
    }
}

/// Eat whitespace
pub fn nom_ws(i: CSpan<'_>) -> CTokenizerResult<'_, CSpan<'_>> {
    i.split_at_position_complete(|item| {
        let c = item.as_char();
        !(c == ' ' || c == '\t')
    })
}

/// Eat whitespace
fn nom_ws1(i: CSpan<'_>) -> CTokenizerResult<'_, CSpan<'_>> {
    take_while1::<_, _, CTokenizerError<'_>>(|c: char| c == ' ' || c == '\t')(i)
        .with_code(CWhitespace)
}

fn consumed_all(i: CSpan<'_>, c: CCode) -> CParserResult<'_, ()> {
    let rest = nom_ws_span(i);
    if !rest.is_empty() {
        return Err(nom::Err::Error(CParserError::new(c, rest)));
    } else {
        return Ok((rest, ()));
    }
}
