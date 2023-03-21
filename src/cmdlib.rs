#![allow(dead_code)]

use crate::cmds::CCode;
use crate::cmds::CCode::{CIgnore, CNomError, CWhitespace};
use kparse::prelude::*;
use kparse::{ParserError, ParserResult, TokenizerError, TokenizerResult};
use nom::bytes::complete::{tag, take_till1, take_while1};
use nom::combinator::{consumed, recognize};
use nom::InputTake;
use nom::{AsChar, InputTakeAtPosition};

define_span!(pub CSpan = CCode, str);
pub type CParserResult<'s, O> = ParserResult<CCode, CSpan<'s>, O>;
pub type CTokenizerResult<'s, O> = TokenizerResult<CCode, CSpan<'s>, O>;
pub type CParserError<'s> = ParserError<CCode, CSpan<'s>>;
pub type CTokenizerError<'s> = TokenizerError<CCode, CSpan<'s>>;

// -----------------------------------------------------------------------
// -----------------------------------------------------------------------

pub struct ParseCmd<O, T> {
    pub to_cmd: fn(O) -> T,
    pub sub: SubCmd<O>,
}

pub struct ParseCmd2<O, T, const N: usize> {
    pub to_cmd: fn(O) -> T,
    pub token: &'static str,
    pub code: CCode,
    pub list: [SubCmd<O>; N],
}

pub struct SubCmd<O> {
    pub token: &'static str,
    pub code: CCode,
    pub to_out: fn(CSpan<'_>) -> CParserResult<'_, O>,
}

// -----------------------------------------------------------------------
// -----------------------------------------------------------------------

impl<O, T> ParseCmd<O, T>
where
    O: Clone,
{
    pub fn parse<'s>(&self, input: CSpan<'s>) -> CParserResult<'s, T> {
        Track.enter(self.sub.code, input);

        match token_command(self.sub.token, self.sub.code, input) {
            Ok((rest, _)) => match (self.sub.to_out)(rest) {
                Ok((rest, sub)) => {
                    consumed_all(rest, self.sub.code)?;
                    return Track.ok(rest, input, (self.to_cmd)(sub));
                }
                Err(e) => {
                    return Track.err(e.with_code(self.sub.code));
                }
            },
            Err(nom::Err::Error(e)) if e.code == CIgnore => {
                return Track.err(e);
            }
            Err(e) => {
                return Track.err(e.with_code(self.sub.code));
            }
        }
    }
}

impl<O, T, const N: usize> ParseCmd2<O, T, N>
where
    O: Clone,
{
    pub fn parse<'s>(&self, input: CSpan<'s>) -> CParserResult<'s, T> {
        Track.enter(self.code, input);

        let (rest, _token) = token_command(self.token, self.code, input).track()?;
        let (rest, _) = nom_ws1(rest).err_into().track()?;

        let mut err: Option<CParserError<'_>> = None;
        for sub in &self.list {
            match token_command(sub.token, sub.code, rest) {
                Ok((rest, _span)) => {
                    match (sub.to_out)(rest) {
                        Ok((rest, sub_o)) => {
                            consumed_all(rest, sub.code)?;
                            return Track.ok(rest, input, (self.to_cmd)(sub_o));
                        }
                        Err(e) => {
                            return Track.err(e.with_code(sub.code).with_code(self.code));
                        }
                    };
                }
                Err(nom::Err::Error(e)) => {
                    if e.code != CIgnore {
                        err.append(e.with_code(sub.code));
                    }
                }
                Err(e) => {
                    return Track.err(e.with_code(sub.code).with_code(self.code));
                }
            }
        }

        match err {
            Some(err) => Track.err(err.with_code(self.code)),
            None => {
                // not even one prefix match. list all.
                let mut err = CParserError::new(self.code, rest);
                for sub in &self.list {
                    err.suggest(sub.code, rest);
                }
                Track.err(err)
            }
        }
    }
}

// -----------------------------------------------------------------------
// -----------------------------------------------------------------------

fn lah_command(tok: &'_ str, rest: CSpan<'_>) -> bool {
    match tag::<_, _, CParserError<'_>>(tok)(rest) {
        Ok(_) => true,
        Err(_) => match nom_last_token(rest) {
            Ok((_, last)) => {
                let last = last.to_lowercase();
                tok.starts_with(&last)
            }
            Err(_) => false,
        },
    }
}

/// Tries to parse the token. If it fails and at least partially matches it adds a Suggest.
fn token_command<'a>(tok: &'_ str, code: CCode, rest: CSpan<'a>) -> CParserResult<'a, CSpan<'a>> {
    let (rest, token) = match tag::<_, _, CParserError<'a>>(tok)(rest) {
        Ok((rest, token)) => (rest, token),
        Err(nom::Err::Error(_) | nom::Err::Failure(_)) => {
            //
            match nom_last_token(rest) {
                Ok((rest, last)) => {
                    let err = if tok.starts_with(&last.to_lowercase()) {
                        let mut err = CParserError::new(code, last);
                        err.suggest(code, last);
                        err
                    } else {
                        CParserError::new(CIgnore, rest)
                    };
                    return Err(nom::Err::Error(err));
                }
                Err(_) => return Err(nom::Err::Error(CParserError::new(CIgnore, rest))),
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
