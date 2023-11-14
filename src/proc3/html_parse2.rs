#![allow(dead_code)]

use crate::proc3::named_char::{NAMED_CHAR, NAMED_CHAR_VAL};
use kparse::combinators::{fchar, fsense, pchar, track};
use kparse::spans::SpanFragment;
use kparse::KParseError;
use kparse::ParseSpan;
use kparse::{define_span, Code, Track};
use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case, take_while1};
use nom::combinator::{opt, recognize};
use nom::error::ParseError;
use nom::sequence::{preceded, terminated, tuple};
use nom::{AsChar, IResult, InputIter, Slice};
use std::fmt::{Display, Formatter};
use std::ops::{RangeFrom, RangeTo};
use std::str::from_utf8_unchecked;

#[derive(Debug, PartialEq, Clone, Copy, Eq)]
pub enum HtmlCode {
    NomError,

    Html,
    Text,
    TextX,
    CData,
    StartTag,
    EndTag,
    CharRef,
    Bogus,
    Comment,
    DocType,
    Eof,
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
    ParseError(Span<'s>),
    Text(Span<'s>),
    StartTag(Span<'s>),
    EndTag(Span<'s>),
    CharRef(char),
    CharRefStr(&'static str),
    Comment(Span<'s>),
    DocType(Span<'s>),
    CData(Span<'s>),
    Eof,
}

define_span!(pub Span = HtmlCode, str);
// pub type Span<'a> = &'a str;
pub type ParserResult<'s, O> = kparse::ParserResult<HtmlCode, Span<'s>, O>;
pub type TokenizerResult<'s> = kparse::TokenizerResult<HtmlCode, Span<'s>, Span<'s>>;
pub type NomResult<'s> = kparse::ParserResult<HtmlCode, Span<'s>, Span<'s>>;
pub type ParserError<'s> = kparse::ParserError<HtmlCode, Span<'s>>;

// todo: bom / wide-char recognition.

pub fn parse_html(input: Span<'_>) -> ParserResult<'_, HtmlPart> {
    track(
        HtmlCode::Html,
        alt((
            parse_text,
            parse_charref,
            parse_comment,
            parse_cdata,
            parse_bogus,
            parse_doctype,
            parse_endtag,
            parse_starttag,
            parse_lt_amp,
            parse_eof,
        )),
    )(input)
    .with_code(HtmlCode::Html)
}

fn parse_lt_amp(input: Span<'_>) -> ParserResult<'_, HtmlPart<'_>> {
    let (rest, v) = track(HtmlCode::TextX, recognize(alt((pchar('<'), pchar('&')))))(input)
        .with_code(HtmlCode::TextX)?;

    Ok((rest, HtmlPart::Text(v)))
}

fn parse_cdata(input: Span<'_>) -> ParserResult<'_, HtmlPart<'_>> {
    let (rest, v) = track(
        HtmlCode::CData,
        recognize(tuple((tag("<!"), tag_no_case("[cdata["), parse_cdata_rest))),
    )(input)
    .with_code(HtmlCode::CData)?;

    Ok((rest, HtmlPart::CData(v)))
}

#[inline]
fn parse_cdata_rest(input: Span<'_>) -> ParserResult<'_, Span<'_>> {
    let mut idx = 0usize;

    let mut it = input.iter_elements();
    'cdata: loop {
        match it.next() {
            None => break 'cdata,
            Some(']') => {
                idx += 1;
                match it.next() {
                    None => break 'cdata,
                    Some(']') => {
                        idx += 1;
                        break 'cdata;
                    }
                    Some(c) => {
                        idx += c.len();
                    }
                }
            }
            Some(c) => {
                idx += c.len();
            }
        }
    }
    Ok((input.slice(idx..), input.slice(..idx)))
}

fn parse_bogus(input: Span<'_>) -> ParserResult<'_, HtmlPart> {
    let (rest, v) = track(HtmlCode::Bogus, recognize(tuple((tag("<?"), unto('>')))))(input)
        .with_code(HtmlCode::Bogus)?;

    Ok((rest, HtmlPart::Comment(v)))
}

fn parse_starttag(input: Span<'_>) -> ParserResult<'_, HtmlPart> {
    let (rest, v) = track(
        HtmlCode::StartTag,
        recognize(tuple((
            pchar('<'),
            fchar(|c| c.is_ascii_alphabetic()),
            unto('>'),
        ))),
    )(input)
    .with_code(HtmlCode::StartTag)?;

    Ok((rest, HtmlPart::StartTag(v)))
}

#[inline]
fn parse_endtag(input: Span<'_>) -> ParserResult<'_, HtmlPart> {
    let (rest, v) = track(
        HtmlCode::EndTag,
        recognize(tuple((
            tag("</"),
            fchar(|c| c.is_ascii_alphabetic()),
            unto('>'),
        ))),
    )(input)
    .with_code(HtmlCode::EndTag)?;

    Ok((rest, HtmlPart::EndTag(v)))
}

#[inline]
fn parse_doctype(input: Span<'_>) -> ParserResult<'_, HtmlPart> {
    let (rest, v) = track(
        HtmlCode::DocType,
        recognize(preceded(tag_no_case("<!doctype"), unto('>'))),
    )(input)
    .with_code(HtmlCode::DocType)?;

    Ok((rest, HtmlPart::DocType(v)))
}

#[inline]
fn parse_comment(input: Span<'_>) -> ParserResult<'_, HtmlPart> {
    let (rest, v) = track(
        HtmlCode::Comment,
        recognize(preceded(tag("<!--"), parse_comment_rest)),
    )(input)?;

    Ok((rest, HtmlPart::Comment(v)))
}

#[inline]
fn parse_comment_rest(input: Span<'_>) -> ParserResult<'_, Span<'_>> {
    let mut idx = 0usize;

    let mut it = input.iter_elements();
    'comment: loop {
        match it.next() {
            None => break 'comment,
            Some('-') => {
                idx += 1;
                match it.next() {
                    None => break 'comment,
                    Some('-') => {
                        idx += 1;
                        match it.next() {
                            None => break 'comment,
                            Some('>') => {
                                idx += 1;
                                break 'comment;
                            }
                            Some('!') => {
                                idx += 1;
                                match it.next() {
                                    None => break 'comment,
                                    Some('-') => {
                                        idx += 1;
                                    }
                                    Some('>') => {
                                        idx += 1;
                                        break 'comment;
                                    }
                                    Some(c) => {
                                        idx += c.len();
                                    }
                                }
                            }
                            Some('-') => {
                                idx += 1;
                            }
                            Some(c) => {
                                idx += c.len();
                            }
                        }
                    }
                    Some('>') => {
                        idx += 1;
                        break 'comment;
                    }
                    Some(c) => {
                        idx += c.len();
                    }
                }
            }
            Some('>') => {
                idx += 1;
                break 'comment;
            }
            Some(c) => {
                idx += c.len();
            }
        }
    }

    Ok((input.slice(idx..), input.slice(..idx)))
}

#[inline]
fn parse_text(input: Span<'_>) -> ParserResult<'_, HtmlPart> {
    let (rest, v) = track(
        HtmlCode::Text,
        recognize(take_while1(|c: char| c != '<' && c != '&' && c != '\0')),
    )(input)
    .with_code(HtmlCode::Text)?;

    Ok((rest, HtmlPart::Text(v)))
}

#[inline]
fn parse_eof(input: Span<'_>) -> ParserResult<'_, HtmlPart> {
    Track.enter(HtmlCode::Eof, input);
    if input.len() == 0 {
        Track.ok(input, input, HtmlPart::Eof)
    } else {
        Track.err(ParserError::new(HtmlCode::Eof, input))
    }
}

#[inline]
fn parse_charref(input: Span<'_>) -> ParserResult<'_, HtmlPart> {
    let (rest, (_, v)) = track(
        HtmlCode::CharRef,
        tuple((
            pchar('&'),
            alt((
                preceded(fsense(|c| c.is_ascii_alphanumeric()), tok_named_charref),
                preceded(tag("#"), tok_dec_charref),
                preceded(tag("#X"), tok_hex_charref),
                preceded(tag("#x"), tok_hex_charref),
            )),
        )),
    )(input)
    .with_code(HtmlCode::CharRef)?;

    Ok((rest, v))
}

#[inline]
fn tok_dec_charref(input: Span<'_>) -> ParserResult<'_, HtmlPart> {
    let (rest, v) = terminated(take_while1(|c: char| c.is_ascii_digit()), opt(pchar(';')))(input)
        .with_code(HtmlCode::CharRef)?;

    match u32::from_str_radix(v.fragment(), 10) {
        Ok(v) => match char::from_u32(v) {
            Some(c) => Ok((rest, HtmlPart::CharRef(c))),
            None => Err(nom::Err::Error(ParserError::new(HtmlCode::CharRef, input))),
        },
        Err(_) => Err(nom::Err::Error(ParserError::new(HtmlCode::CharRef, input))),
    }
}

#[inline]
fn tok_hex_charref(input: Span<'_>) -> ParserResult<'_, HtmlPart> {
    let (rest, v) = terminated(
        take_while1(|c: char| c.is_ascii_hexdigit()),
        opt(pchar(';')),
    )(input)
    .with_code(HtmlCode::CharRef)?;

    match u32::from_str_radix(v.fragment(), 16) {
        Ok(v) => match char::from_u32(v) {
            Some(c) => Ok((rest, HtmlPart::CharRef(c))),
            None => Err(nom::Err::Error(ParserError::new(HtmlCode::CharRef, input))),
        },
        Err(_) => Err(nom::Err::Error(ParserError::new(HtmlCode::CharRef, input))),
    }
}

#[inline]
fn tok_named_charref(input: Span<'_>) -> ParserResult<'_, HtmlPart> {
    let mut name = [0u8; 32];
    let mut ins = 0usize;

    for c in input.iter_elements() {
        if c as u32 > 256 {
            break;
        }

        name[ins] = c as u8;

        let find = &name[..ins + 1];
        if let Ok(idx) = NAMED_CHAR.binary_search(&find) {
            return Ok((
                input.slice(0..ins + 1),
                HtmlPart::CharRefStr(unsafe { from_utf8_unchecked(NAMED_CHAR_VAL[idx]) }),
            ));
        }

        if c == ';' {
            break;
        }

        ins += 1;

        if ins >= name.len() {
            break;
        }
    }

    Err(nom::Err::Error(ParserError::new(HtmlCode::CharRef, input)))
}

// parse up to and including the character. consumes the whole input if no such character is found.
#[inline]
fn unto<I, Error: ParseError<I>>(cc: <I as InputIter>::Item) -> impl Fn(I) -> IResult<I, I, Error>
where
    I: Slice<RangeTo<usize>> + Slice<RangeFrom<usize>> + InputIter,
    <I as InputIter>::Item: PartialEq,
    <I as InputIter>::Item: AsChar,
{
    move |i: I| {
        let mut idx = 0usize;

        let mut it = i.iter_elements();
        'endtag: loop {
            match it.next() {
                None => break 'endtag,
                Some(c) if c == cc => {
                    idx += c.len();
                    break 'endtag;
                }
                Some(c) => {
                    idx += c.len();
                }
            }
        }

        Ok((i.slice(idx..), i.slice(..idx)))
    }
}

#[cfg(test)]
mod tests {
    use crate::proc3::html_parse2::{
        parse_bogus, parse_cdata, parse_doctype, parse_endtag, parse_html, parse_starttag,
        parse_text, HtmlPart, Span,
    };
    use kparse::test::{str_parse, CheckTrace, Trace};

    const R: Trace = Trace;

    #[test]
    fn test_cdata() {
        str_parse(&mut None, "<![CDATA[>", parse_cdata)
            .ok_any()
            .q(R);
        str_parse(&mut None, "<![CDATA[]>", parse_cdata)
            .ok_any()
            .q(R);
        str_parse(&mut None, "<![CDATA[]]>", parse_cdata)
            .ok_any()
            .q(R);
        str_parse(&mut None, "<", parse_cdata).err_any().q(R);
        str_parse(&mut None, "<!", parse_cdata).err_any().q(R);
        str_parse(&mut None, "<![", parse_cdata).err_any().q(R);
        str_parse(&mut None, "<![CDATA", parse_cdata).err_any().q(R);
        str_parse(&mut None, "<![CDATA[", parse_cdata).ok_any().q(R);
        str_parse(&mut None, "<![CDATA[blabla", parse_cdata)
            .ok_any()
            .q(R);
        str_parse(&mut None, "<![CDATA[blabla]", parse_cdata)
            .ok_any()
            .q(R);
        str_parse(&mut None, "<![CDATA[blabla]]", parse_cdata)
            .ok_any()
            .q(R);
    }

    #[test]
    fn test_bogus() {
        str_parse(&mut None, "<?", parse_bogus).ok_any().q(R);
        str_parse(&mut None, "<?asdfasdf>asdf", parse_bogus)
            .ok_any()
            .q(R);
    }

    #[test]
    fn test_starttag() {
        str_parse(&mut None, "<?", parse_starttag).err_any().q(R);
        str_parse(&mut None, "<!", parse_starttag).err_any().q(R);
        str_parse(&mut None, "<--", parse_starttag).err_any().q(R);
        str_parse(&mut None, "<a", parse_starttag).ok_any().q(R);
        str_parse(&mut None, "<a href=\"&lt&gt\">", parse_starttag)
            .ok_any()
            .q(R);
    }

    #[test]
    fn test_endtag() {
        str_parse(&mut None, "</asdf", parse_endtag).ok_any().q(R);
        str_parse(&mut None, "</asdf>", parse_endtag).ok_any().q(R);
        str_parse(&mut None, "</ FONT>", parse_endtag)
            .err_any()
            .q(R);
    }

    #[test]
    fn test_doctype() {
        str_parse(&mut None, "<!doctype", parse_doctype)
            .ok_any()
            .q(R);
        str_parse(&mut None, "<!doctype bla bla>", parse_doctype)
            .ok_any()
            .q(R);
    }

    #[test]
    fn test_comment() {
        str_parse(&mut None, "<!--doctype", parse_doctype)
            .ok_any()
            .q(R);
        str_parse(&mut None, "<!-->", parse_doctype).ok_any().q(R);
        str_parse(&mut None, "<!--->", parse_doctype).ok_any().q(R);
        str_parse(&mut None, "<!---->", parse_doctype).ok_any().q(R);
        str_parse(&mut None, "<!----->", parse_doctype)
            .ok_any()
            .q(R);
        str_parse(&mut None, "<!------>", parse_doctype)
            .ok_any()
            .q(R);
        str_parse(&mut None, "<!--!---->", parse_doctype)
            .ok_any()
            .q(R);
        str_parse(&mut None, "<!--!>", parse_doctype).ok_any().q(R);
    }

    #[test]
    fn test_text() {
        fn eq(p: &HtmlPart<'_>, t: &'static str) -> bool {
            match p {
                HtmlPart::Text(v) => *v.fragment() == t,
                _ => false,
            }
        }

        str_parse(&mut None, "tex&tex", parse_text)
            .ok(eq, "tex")
            .q(R);
        str_parse(&mut None, "tex<tex", parse_text)
            .ok(eq, "tex")
            .q(R);
        str_parse(&mut None, "t\0e\0x\0<\0t\0e\0x\0", parse_text)
            .ok(eq, "tex")
            .q(R);
    }

    #[test]
    fn test_html() {
        str_parse(
            &mut None,
            "*---(:>     MrDouble's Palisades     <:)---*",
            parse_html,
        )
        .ok_any()
        .q(R);
        str_parse(&mut None, "<:)---*", parse_html).ok_any().q(R);
    }
}
