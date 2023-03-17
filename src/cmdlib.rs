use crate::cmds::CCode::{CIgnore, CNomError, CWhitespace};
use crate::cmds::{BCommand, CCode};
use kparse::prelude::*;
use kparse::{ParserError, ParserResult, TokenizerError, TokenizerResult};
use nom::bytes::complete::{tag, take_till1, take_while1};
use nom::combinator::{consumed, recognize};
use nom::InputTake;
use nom::{AsChar, InputTakeAtPosition};
use nom_locate::LocatedSpan;
use std::fmt::Debug;

// define_span!(pub CSpan = CCode, str);
pub type CSpan<'a> = LocatedSpan<&'a str, &'a (dyn TrackProvider<CCode, &'a str>)>;
pub type CParserResult<'s, O> = ParserResult<CCode, CSpan<'s>, O>;
pub type CTokenizerResult<'s, O> = TokenizerResult<CCode, CSpan<'s>, O>;
pub type CParserError<'s> = ParserError<CCode, CSpan<'s>>;
pub type CTokenizerError<'s> = TokenizerError<CCode, CSpan<'s>>;

// Generic parsers -------------------------------------------------------

pub struct Parse1LayerCommand {
    pub cmd: BCommand,
    pub layers: Parse1Layers,
}

impl Parse1LayerCommand {
    fn id(&self) -> CCode {
        self.layers.code
    }

    pub(crate) fn lah(&self, span: CSpan<'_>) -> bool {
        lah_command(self.layers.token, span)
    }

    pub(crate) fn parse<'s>(&self, rest: CSpan<'s>) -> CParserResult<'s, BCommand> {
        Track.enter(self.id(), rest);

        let (rest, sub) = self.layers.parse(rest).track()?;

        let rest = nom_ws_span(rest);

        if !rest.is_empty() {
            return Track.err(CParserError::new(self.id(), rest));
        }

        Track.ok(rest, sub, self.cmd.clone())
    }
}

pub struct Parse1Layers {
    pub token: &'static str,
    pub code: CCode,
}

impl Parse1Layers {
    fn id(&self) -> CCode {
        self.code
    }

    fn parse<'s>(&self, rest: CSpan<'s>) -> CParserResult<'s, CSpan<'s>> {
        Track.enter(self.id(), rest);

        let (rest, token) = token_command(self.token, self.code, rest)
            .err_into()
            .track()?;

        Track.ok(rest, token, token)
    }
}

pub struct Parse2LayerCommand<O: Clone + Debug, const N: usize> {
    pub map_cmd: fn(O) -> BCommand,
    pub layers: Parse2Layers<O, N>,
}

impl<O: Clone + Debug, const N: usize> Parse2LayerCommand<O, N> {
    pub(crate) fn lah(&self, span: CSpan<'_>) -> bool {
        lah_command(self.layers.token, span)
    }

    pub(crate) fn parse<'s>(&self, rest: CSpan<'s>) -> CParserResult<'s, BCommand> {
        Track.enter(self.layers.code, rest);

        let (rest, (span, sub)) = self.layers.parse(rest).track()?;

        let rest = nom_ws_span(rest);

        if !rest.is_empty() {
            return Track.err(CParserError::new(self.layers.code, rest));
        }

        Track.ok(rest, span, (self.map_cmd)(sub))
    }
}

pub struct Parse2Layers<O: Clone, const N: usize> {
    pub token: &'static str,
    pub code: CCode,
    pub list: [SubCmd<O>; N],
}

pub struct SubCmd<O: Clone> {
    pub token: &'static str,
    pub code: CCode,
    pub output: fn(CSpan<'_>) -> CParserResult<'_, O>,
}

impl<O: Clone, const N: usize> Parse2Layers<O, N> {
    fn parse<'s>(&self, input: CSpan<'s>) -> CParserResult<'s, (CSpan<'s>, O)> {
        Track.enter(self.code, input);

        let (rest, _token) = token_command(self.token, self.code, input)
            .err_into()
            .track()?;

        let (rest, _) = nom_ws1(rest).err_into().track()?;

        let mut err: Option<CParserError<'_>> = None;
        for sub in &self.list {
            match token_command(sub.token, sub.code, rest) {
                Ok((rest, _span)) => {
                    return match consumed(sub.output)(rest) {
                        Ok((rest, (span_o, sub_o))) => Track.ok(rest, input, (span_o, sub_o)),
                        Err(e) => Track.err(e.with_code(sub.code).with_code(self.code)),
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
fn nom_last_token(i: CSpan<'_>) -> CTokenizerResult<'_, CSpan<'_>> {
    match recognize::<_, _, CTokenizerError<'_>, _>(take_till1(|c: char| c == ' ' || c == '\t'))(i)
    {
        Ok((rest, tok)) if rest.is_empty() => Ok((rest, tok)),
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
