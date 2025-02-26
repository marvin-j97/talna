use super::span::{Parse, ParseResult, RawSpan};
use nom::{
    branch::alt,
    bytes::complete::{tag, take_while},
    character::complete::{digit1, space0},
    combinator::opt,
    multi::separated_list1,
    sequence::{delimited, terminated},
};
use std::ops::Bound::{self, Excluded, Included, Unbounded};

#[derive(Debug, Eq, PartialEq)]
pub enum TagValue<'a> {
    String(&'a str),
    Set(Vec<Self>),
    Integer(u64),
    IntegerRange((Bound<u64>, Bound<u64>)),
}

impl<'a> TagValue<'a> {
    fn parse_atom(input: RawSpan<'a>) -> ParseResult<'a, Self> {
        let (input, value) =
            take_while(|x: char| x.is_alphanumeric() || x == '_' || x == '-')(input)?;

        Ok((
            input,
            value
                .fragment()
                .parse::<u64>()
                .map_or_else(|_| Self::String(&value), Self::Integer),
        ))
    }

    fn parse_closed_range(input: RawSpan<'a>) -> ParseResult<'a, Self> {
        let (input, a) = digit1(input)?;
        let (input, _) = delimited(space0, tag(".."), space0)(input)?;
        let (input, inc) = opt(terminated(tag("="), space0))(input)?;
        let (input, b) = digit1(input)?;

        let a = a.fragment().parse::<u64>().expect("invalid number");
        let b = b.fragment().parse::<u64>().expect("invalid number");

        let b = if inc.is_some() {
            Included(b)
        } else {
            Excluded(b)
        };

        Ok((input, Self::IntegerRange((Included(a), b))))
    }

    fn parse_half_open_range(input: RawSpan<'a>) -> ParseResult<'a, Self> {
        let (input, n) = digit1(input)?;
        let (input, _) = delimited(space0, tag(".."), space0)(input)?;
        let n = n.fragment().parse::<u64>().expect("invalid number");

        Ok((input, Self::IntegerRange((Included(n), Unbounded))))
    }

    fn parse_full_range(input: RawSpan<'a>) -> ParseResult<'a, Self> {
        let (input, _) = tag("..")(input)?;

        Ok((input, Self::IntegerRange((Unbounded, Unbounded))))
    }

    fn parse_range(input: RawSpan<'a>) -> ParseResult<'a, Self> {
        let (input, _) = tag("[")(input)?;
        let (input, result) = delimited(
            space0,
            alt((
                Self::parse_closed_range,
                Self::parse_half_open_range,
                Self::parse_full_range,
            )),
            space0,
        )(input)?;
        let (input, _) = tag("]")(input)?;
        Ok((input, result))
    }

    pub fn parse_set(input: RawSpan<'a>) -> ParseResult<'a, Self> {
        let (input, _) = tag("[")(input)?;

        let (input, values) =
            separated_list1(delimited(space0, tag(","), space0), Self::parse_atom)(input)?;

        let (input, _) = tag("]")(input)?;

        let set = TagValue::Set(values.into_iter().collect());

        Ok((input, set))
    }
}

impl<'a> Parse<'a> for TagValue<'a> {
    fn parse(input: RawSpan<'a>) -> ParseResult<'a, Self> {
        let (input, result) = alt((Self::parse_range, Self::parse_set, Self::parse_atom))(input)?;
        Ok((input, result))
    }
}
