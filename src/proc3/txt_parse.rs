use kparse::combinators::{pchar, track};
use kparse::spans::SpanFragment;
use kparse::KParseError;
use kparse::{define_span, Code, ErrInto, ParseSpan, TokenizerError, Track, TrackResult};
use nom::branch::alt;
use nom::bytes::complete::{tag, take_while, take_while1, take_while_m_n};
use nom::character::complete::one_of;
use nom::combinator::recognize;
use nom::sequence::{preceded, terminated, tuple};
use nom::{InputIter, InputTake, Slice};
use std::fmt::{Debug, Display, Formatter};

#[derive(Debug, PartialEq, Clone, Copy, Eq)]
pub enum TxtCode {
    NomError,

    Text,

    Word,
    Pgp,
    Base64,
    KeyValue,
    Tag,

    WordTok,
    NonWord,
    Base64Begin,
    Base64Line,
    Base64Stop,
    Base64End,
    PgpStart,
    PgpEnd,
    PgpSpecial,
    Key,
    Any,
    AtNewline,
    NewLine,
    WhiteSpace,
    Eof,
}

impl Display for TxtCode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Code for TxtCode {
    const NOM_ERROR: Self = Self::NomError;
}

#[derive(Debug)]
pub enum TxtPart<'s> {
    Text(Span<'s>),
    Pgp,
    Base64,
    KeyValue,
    Tag,
    NonText,
    NewLine,
    Eof,
}

define_span!(Span = TxtCode, str);
// type Span<'a> = &'a str;
pub type ParserResult<'s, O> = kparse::ParserResult<TxtCode, Span<'s>, O>;
pub type TokenizerResult<'s> = kparse::TokenizerResult<TxtCode, Span<'s>, Span<'s>>;
pub type NomResult<'s> = kparse::ParserResult<TxtCode, Span<'s>, Span<'s>>;
pub type ParserError<'s> = kparse::ParserError<TxtCode, Span<'s>>;

pub fn parse_txt(input: Span<'_>) -> ParserResult<'_, TxtPart> {
    Track.enter(TxtCode::Text, input);

    let rest = input;

    // at beginning of line
    let (rest, v) = newline(rest).err_into().track()?;
    let (rest, v) = if !v.is_empty() {
        let (rest, _) = whitespace(rest).err_into()?;
        match alt((parse_pgp, parse_base64, parse_key_value))(rest) {
            Ok((rest, v)) => (rest, Some(v)),
            Err(_) => (rest, None),
        }
    } else {
        (rest, None)
    };

    if let Some(v) = v {
        return Track.ok(rest, input, v);
    }

    let (rest, v) = alt((
        parse_tag,
        parse_word,
        parse_nonword,
        parse_newline,
        parse_eof,
    ))(input)
    .track()?;

    Track.ok(rest, input, v)
}

#[inline]
pub fn parse_eof(input: Span<'_>) -> ParserResult<'_, TxtPart> {
    Track.enter(TxtCode::Eof, input);
    if input.len() == 0 {
        Track.ok(input, input, TxtPart::Eof)
    } else {
        Track.err(ParserError::new(TxtCode::Eof, input))
    }
}

#[inline]
pub fn parse_newline(input: Span<'_>) -> ParserResult<'_, TxtPart> {
    let (rest, _v) = track(TxtCode::NewLine, newline)(input)
        .with_code(TxtCode::NewLine)
        .err_into()?;
    Ok((rest, TxtPart::NewLine))
}

#[inline]
pub fn parse_tag(input: Span<'_>) -> ParserResult<'_, TxtPart> {
    let (rest, _) = track::<_, _, _, _, ParserError>(
        TxtCode::Tag,
        tuple((
            pchar('<'),
            take_while_m_n(1, 20, |v: char| v != '>'),
            pchar('>'),
        )),
    )(input)
    .err_into()?;

    Ok((rest, TxtPart::Tag))
}

#[inline]
pub fn parse_word(input: Span<'_>) -> ParserResult<'_, TxtPart> {
    let (rest, v) = track(TxtCode::Word, terminated(tok_word, tok_non_word0))(input)
        .with_code(TxtCode::Word)
        .err_into()?;
    Ok((rest, TxtPart::Text(v)))
}

#[inline]
pub fn parse_nonword(input: Span<'_>) -> ParserResult<'_, TxtPart> {
    let (rest, _v) = track(TxtCode::NonWord, tok_non_word1)(input)
        .with_code(TxtCode::Word)
        .err_into()?;
    Ok((rest, TxtPart::NonText))
}

#[inline]
pub fn tok_word(input: Span<'_>) -> TokenizerResult<'_> {
    track(
        TxtCode::WordTok,
        recognize(take_while1(|c: char| c.is_alphabetic())),
    )(input)
    .with_code(TxtCode::Word)
}

#[inline]
pub fn tok_non_word1(input: Span<'_>) -> TokenizerResult<'_> {
    track(
        TxtCode::NonWord,
        recognize(take_while1(|c: char| !c.is_alphabetic() && c != '\n')),
    )(input)
    .with_code(TxtCode::NonWord)
}

#[inline]
pub fn tok_non_word0(input: Span<'_>) -> TokenizerResult<'_> {
    track(
        TxtCode::NonWord,
        recognize(take_while(|c: char| !c.is_alphabetic() && c != '\n')),
    )(input)
    .with_code(TxtCode::NonWord)
}

#[inline]
pub fn parse_pgp(input: Span<'_>) -> ParserResult<'_, TxtPart> {
    Track.enter(TxtCode::Pgp, input);
    let (rest, v) = recognize(tuple((
        tag("-----BEGIN PGP SIGNATURE-----"),
        tok_any_until_new_line,
        newline,
        //
        tok_pgp_text,
        //
        tag("END PGP SIGNATURE-----"),
        tok_any_until_new_line,
    )))(input)
    .with_code(TxtCode::Pgp)
    .err_into()
    .track()?;
    Track.ok(rest, input, TxtPart::Pgp)
}

#[inline]
pub fn tok_pgp_text(input: Span<'_>) -> TokenizerResult<'_> {
    Track.enter(TxtCode::PgpSpecial, input);

    let mut it = input.iter_indices();
    'l: loop {
        match it.next() {
            Some((pos, '-')) => {
                for _ in 0..4 {
                    if let Some((_pos, c)) = it.next() {
                        if c != '-' {
                            continue 'l;
                        }
                    } else {
                        continue 'l;
                    }
                }
                return Track.ok(input.slice(pos + 5..), input, input.slice(..pos + 5));
            }
            Some((_, _)) => {}
            None => {
                return Track.err(TokenizerError::new(TxtCode::PgpSpecial, input));
            }
        }
    }
}

#[inline]
pub fn parse_base64(input: Span<'_>) -> ParserResult<'_, TxtPart> {
    let rest = input;

    let (rest, _v) = tok_base64_begin(rest).err_into()?;

    let mut rest2 = rest;
    loop {
        let (rest3, v) = alt((
            preceded(newline, tok_base64_stop),
            preceded(newline, tok_base64_line),
        ))(rest2)
        .err_into()?;

        rest2 = rest3;

        if *v.fragment() == "`" {
            break;
        }
    }
    let rest = rest2;

    let (rest, _v) = tok_base64_end(rest).err_into()?;

    Ok((rest, TxtPart::Base64))
}

#[inline]
pub fn tok_base64_begin(input: Span<'_>) -> TokenizerResult<'_> {
    track(
        TxtCode::Base64Begin,
        recognize(tuple((tag("begin"), tok_any_until_new_line))),
    )(input)
    .with_code(TxtCode::Base64Begin)
}

#[inline]
pub fn tok_base64_line(input: Span<'_>) -> TokenizerResult<'_> {
    track(
        TxtCode::Base64Line,
        recognize(tuple((whitespace, tok_any_until_new_line1))),
    )(input)
    .with_code(TxtCode::Base64Line)
}

#[inline]
pub fn tok_base64_stop(input: Span<'_>) -> TokenizerResult<'_> {
    track(
        TxtCode::Base64Stop,
        recognize(tuple((pchar('`'), tok_at_new_line))),
    )(input)
    .with_code(TxtCode::Base64Stop)
}

#[inline]
pub fn tok_base64_end(input: Span<'_>) -> TokenizerResult<'_> {
    track(
        TxtCode::Base64End,
        recognize(tuple((whitespace, tag("end"), tok_any_until_new_line))),
    )(input)
    .with_code(TxtCode::Base64End)
}

#[inline]
pub fn parse_key_value(input: Span<'_>) -> ParserResult<'_, TxtPart> {
    let (rest, v) = track(
        TxtCode::KeyValue,
        recognize(tuple((tok_key, pchar(':'), tok_any_until_new_line))),
    )(input)
    .with_code(TxtCode::KeyValue)
    .err_into()?;

    Ok((rest, TxtPart::KeyValue))
}

#[inline]
pub fn tok_key(input: Span<'_>) -> TokenizerResult<'_> {
    Track.enter(TxtCode::Key, input);
    let (rest, v) = take_while(|c: char| c.is_ascii_alphanumeric() || c == '-' || c == '*')(input)
        .with_code(TxtCode::Key)
        .track()?;
    Track.ok(rest, input, v)
}

#[inline]
pub fn tok_any_until_new_line1(input: Span<'_>) -> TokenizerResult<'_> {
    Track.enter(TxtCode::Any, input);
    let (rest, v) = take_while1(|c: char| c != '\n')(input)
        .with_code(TxtCode::Any)
        .track()?;
    Track.ok(rest, input, v)
}

#[inline]
pub fn tok_any_until_new_line(input: Span<'_>) -> TokenizerResult<'_> {
    Track.enter(TxtCode::Any, input);
    let (rest, v) = take_while(|c: char| c != '\n')(input)
        .with_code(TxtCode::Any)
        .track()?;
    Track.ok(rest, input, v)
}

#[inline]
pub fn tok_at_new_line(input: Span<'_>) -> TokenizerResult<'_> {
    Track.enter(TxtCode::AtNewline, input);
    match input.iter_elements().next() {
        Some('\n') => Track.ok(input, input, input.take(0)),
        _ => Track.err(TokenizerError::new(TxtCode::AtNewline, input)),
    }
}

#[inline]
pub fn newline(input: Span<'_>) -> TokenizerResult<'_> {
    recognize(one_of("\n\r"))(input).with_code(TxtCode::NewLine)
}

#[inline]
pub fn whitespace(input: Span<'_>) -> TokenizerResult<'_> {
    take_while(|c: char| c == ' ' || c == '\t' || c.is_whitespace())(input)
        .with_code(TxtCode::WhiteSpace)
}
