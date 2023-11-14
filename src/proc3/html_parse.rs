use kparse::combinators::{pchar, track};
use kparse::KParseError;
use kparse::ParseSpan;
use kparse::{define_span, Code, Track};
use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case, take_while, take_while1};
use nom::combinator::{opt, recognize};
use nom::error::ParseError;
use nom::multi::many0;
use nom::sequence::{preceded, terminated, tuple};
use nom::{AsChar, IResult, InputIter, InputTake, Slice};
use std::fmt::{Display, Formatter};
use std::ops::{RangeFrom, RangeTo};

#[derive(Debug, PartialEq, Clone, Copy, Eq)]
pub enum HtmlCode {
    NomError,

    Html,
    Text,
    StartTag,
    EndTag,
    Comment,
    CData,
    DocType,
    CharRef,
    Xml,
    Eof,

    DashDash,
}

impl Display for HtmlCode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Code for HtmlCode {
    const NOM_ERROR: Self = Self::NomError;
}

#[derive(Debug)]
pub enum HtmlPart<'s> {
    Text(Span<'s>),
    StartTag(Span<'s>),
    EndTag(Span<'s>),
    DocType(Span<'s>),
    CData(Span<'s>),
    CharRef(Span<'s>),
    Comment(Span<'s>),
    Xml(Span<'s>),
    Eof,
}

define_span!(pub Span = HtmlCode, str);
// pub type Span<'a> = &'a str;
pub type ParserResult<'s, O> = kparse::ParserResult<HtmlCode, Span<'s>, O>;
pub type TokenizerResult<'s> = kparse::TokenizerResult<HtmlCode, Span<'s>, Span<'s>>;
pub type NomResult<'s> = kparse::ParserResult<HtmlCode, Span<'s>, Span<'s>>;
pub type ParserError<'s> = kparse::ParserError<HtmlCode, Span<'s>>;

pub fn parse_html(input: Span<'_>) -> ParserResult<'_, HtmlPart> {
    track(
        HtmlCode::Html,
        alt((
            parse_charref,
            parse_comment,
            parse_cdata,
            parse_xmlheader,
            parse_doctype,
            parse_endtag,
            parse_starttag,
            parse_text,
            parse_eof,
        )),
    )(input)
    .with_code(HtmlCode::Html)
}

pub fn parse_text(input: Span<'_>) -> ParserResult<'_, HtmlPart> {
    let (rest, v) = track(
        HtmlCode::Text,
        recognize(take_while1(|c: char| c != '<' && c != '&')),
    )(input)
    .with_code(HtmlCode::Text)?;

    Ok((rest, HtmlPart::Text(v)))
}

pub fn parse_starttag(input: Span<'_>) -> ParserResult<'_, HtmlPart> {
    let (rest, v) = track(
        HtmlCode::StartTag,
        recognize(tuple((
            pchar('<'),
            fchar(|c| c.is_ascii_alphabetic()),
            take_while(|c: char| c != '>'),
            pchar('>'),
        ))),
    )(input)
    .with_code(HtmlCode::StartTag)?;

    Ok((rest, HtmlPart::StartTag(v)))
}

pub fn parse_endtag(input: Span<'_>) -> ParserResult<'_, HtmlPart> {
    let (rest, v) = track(
        HtmlCode::EndTag,
        recognize(tuple((
            pchar('<'),
            pchar('/'),
            fchar(|c| c.is_ascii_alphabetic()),
            take_while(|c: char| c != '>'),
            pchar('>'),
        ))),
    )(input)
    .with_code(HtmlCode::EndTag)?;

    Ok((rest, HtmlPart::EndTag(v)))
}

pub fn parse_xmlheader(input: Span<'_>) -> ParserResult<'_, HtmlPart> {
    let (rest, v) = track(
        HtmlCode::Xml,
        recognize(tuple((
            pchar('<'),
            pchar('?'),
            pchar('x'),
            pchar('m'),
            pchar('l'),
            many0(tok_not_question_greater),
            pchar('?'),
            pchar('>'),
        ))),
    )(input)
    .with_code(HtmlCode::Xml)?;

    Ok((rest, HtmlPart::Xml(v)))
}

pub fn parse_doctype(input: Span<'_>) -> ParserResult<'_, HtmlPart> {
    let (rest, v) = track(
        HtmlCode::DocType,
        recognize(tuple((
            pchar('<'),
            pchar('!'),
            tag_no_case("doctype"),
            take_while1(|c: char| c != '>'),
        ))),
    )(input)
    .with_code(HtmlCode::DocType)?;

    Ok((rest, HtmlPart::DocType(v)))
}

pub fn parse_comment(input: Span<'_>) -> ParserResult<'_, HtmlPart> {
    let (rest, v) = track(
        HtmlCode::Comment,
        recognize(tuple((
            pchar('<'),
            pchar('!'),
            pchar('-'),
            pchar('-'),
            many0(tok_not_dash_dash_greater),
            pchar('-'),
            pchar('-'),
            pchar('>'),
        ))),
    )(input)?;

    Ok((rest, HtmlPart::Comment(v)))
}

pub fn parse_cdata(input: Span<'_>) -> ParserResult<'_, HtmlPart> {
    let (rest, v) = track(
        HtmlCode::CData,
        recognize(tuple((
            pchar('<'),
            pchar('-'),
            pchar('-'),
            tag_no_case("[cdata["),
            take_while1(|c: char| c != ']'),
            pchar(']'),
            pchar(']'),
        ))),
    )(input)
    .with_code(HtmlCode::CData)?;

    Ok((rest, HtmlPart::CData(v)))
}

pub fn parse_charref(input: Span<'_>) -> ParserResult<'_, HtmlPart> {
    let (rest, v) = track(
        HtmlCode::CharRef,
        recognize(tuple((
            pchar('&'),
            alt((
                terminated(
                    take_while1(|c: char| c.is_ascii_alphanumeric()),
                    opt(pchar(';')),
                ),
                preceded(tag("#"), take_while1(|c: char| c.is_ascii_digit())),
                preceded(tag("#X"), take_while1(|c: char| c.is_ascii_hexdigit())),
                preceded(tag("#x"), take_while1(|c: char| c.is_ascii_hexdigit())),
                tok_empty,
            )),
        ))),
    )(input)
    .with_code(HtmlCode::CharRef)?;

    Ok((rest, HtmlPart::CharRef(v)))
}

#[inline]
pub fn parse_eof(input: Span<'_>) -> ParserResult<'_, HtmlPart> {
    Track.enter(HtmlCode::Eof, input);
    if input.len() == 0 {
        Track.ok(input, input, HtmlPart::Eof)
    } else {
        Track.err(ParserError::new(HtmlCode::Eof, input))
    }
}

/// Same as nom::char but return the input type instead of the char.
#[inline]
pub fn fchar<I, FN, Error: ParseError<I>>(c_fn: FN) -> impl Fn(I) -> IResult<I, I, Error>
where
    I: Slice<RangeTo<usize>> + Slice<RangeFrom<usize>> + InputIter,
    <I as InputIter>::Item: AsChar,
    FN: Fn(char) -> bool,
{
    move |i: I| match i.iter_elements().next() {
        None => Err(nom::Err::Error(Error::from_error_kind(
            i,
            nom::error::ErrorKind::Char,
        ))),
        Some(v) => {
            let cc = v.as_char();
            if c_fn(cc) {
                Ok((i.slice(cc.len()..), i.slice(..cc.len())))
            } else {
                Err(nom::Err::Error(Error::from_error_kind(
                    i,
                    nom::error::ErrorKind::Char,
                )))
            }
        }
    }
}

#[inline]
pub fn tok_not_dash_dash_greater(input: Span<'_>) -> ParserResult<'_, Span<'_>> {
    if input.len() < 1 {
        return Err(nom::Err::Error(ParserError::new(HtmlCode::Eof, input)));
    } else if input.starts_with("-->") {
        return Err(nom::Err::Error(ParserError::new(HtmlCode::DashDash, input)));
    } else {
        Ok(input.take_split(1))
    }
}

#[inline]
pub fn tok_not_question_greater(input: Span<'_>) -> ParserResult<'_, Span<'_>> {
    if input.len() < 1 {
        return Err(nom::Err::Error(ParserError::new(HtmlCode::Eof, input)));
    } else if input.starts_with("?>") {
        return Err(nom::Err::Error(ParserError::new(HtmlCode::DashDash, input)));
    } else {
        Ok(input.take_split(1))
    }
}

#[inline]
pub fn tok_empty(input: Span<'_>) -> ParserResult<'_, Span<'_>> {
    Ok(input.take_split(0))
}
