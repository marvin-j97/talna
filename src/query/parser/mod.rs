mod span;

use super::filter::{Node, Tag};
use nom::{
    branch::alt,
    bytes::complete::{tag, take_while},
    character::complete::space0,
    multi::separated_list1,
    sequence::delimited,
};
use nom_locate::position;
use span::{Parse, ParseResult, Position, RawSpan};

#[derive(Debug, Eq, PartialEq)]
pub enum TagValue<'a> {
    Identifier(&'a str),
    Set(Vec<&'a str>),
}

impl<'a> TagValue<'a> {
    pub fn parse_identifier_raw(input: RawSpan<'a>) -> ParseResult<'a, &str> {
        let (input, value) =
            take_while(|x: char| x.is_alphanumeric() || x == '_' || x == '-')(input)?;
        Ok((input, value.fragment()))
    }

    pub fn parse_identifier(input: RawSpan<'a>) -> ParseResult<'a, Self> {
        let (input, value) = Self::parse_identifier_raw(input)?;
        Ok((input, TagValue::Identifier(value)))
    }

    pub fn parse_set(input: RawSpan<'a>) -> ParseResult<'a, Self> {
        let (input, _) = tag("[")(input)?;

        let (input, values) = separated_list1(
            delimited(space0, tag(","), space0),
            Self::parse_identifier_raw,
        )(input)?;

        let (input, _) = tag("]")(input)?;

        let values = values.into_iter().collect();

        Ok((input, TagValue::Set(values)))
    }
}

impl<'a> Parse<'a> for TagValue<'a> {
    fn parse(input: RawSpan<'a>) -> ParseResult<'a, Self> {
        let (input, result) = alt((Self::parse_set, Self::parse_identifier))(input)?;
        Ok((input, result))
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct ParsedTag<'a> {
    pub key: &'a str,
    pub value: TagValue<'a>,
    position: Position,
}

impl<'a> From<ParsedTag<'a>> for Node<'a> {
    fn from(val: ParsedTag<'a>) -> Self {
        match val.value {
            TagValue::Identifier(value) => Node::Eq(Tag {
                key: val.key,
                value,
            }),
            TagValue::Set(vs) => Node::Or(
                vs.into_iter()
                    .map(|value| {
                        Node::Eq(Tag {
                            key: val.key,
                            value,
                        })
                    })
                    .collect(),
            ),
        }
    }
}

impl<'a> Parse<'a> for ParsedTag<'a> {
    fn parse(input: RawSpan<'a>) -> ParseResult<'a, Self> {
        let (input, pos) = position(input)?;

        let (input, key) = take_while(|x: char| x.is_alphanumeric() || x == '_')(input)?;
        let (input, _) = tag(":")(input)?;
        let (input, value) = TagValue::parse(input)?;

        Ok((
            input,
            Self {
                key: key.fragment(),
                value,
                position: pos.into(),
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    fn parse_tag_simple() {
        let str = "host:a1";

        let (_, tag) = ParsedTag::parse_from_raw(str).unwrap();

        assert_eq!(
            ParsedTag {
                key: "host",
                value: TagValue::Identifier("a1"),
                position: Position { offset: 1, line: 1 }
            },
            tag
        )
    }

    #[test]
    fn parse_tag_simple_2() {
        let str = "host_name:t-128";

        let (_, tag) = ParsedTag::parse_from_raw(str).unwrap();

        assert_eq!(
            ParsedTag {
                key: "host_name",
                value: TagValue::Identifier("t-128"),
                position: Position { offset: 1, line: 1 }
            },
            tag
        )
    }

    #[test]
    fn parse_tag_set() {
        let str = "host:[a1, a2, a3]";

        let (_, tag) = ParsedTag::parse_from_raw(str).unwrap();

        assert_eq!(
            ParsedTag {
                key: "host",
                value: TagValue::Set(vec!["a1", "a2", "a3"]),
                position: Position { offset: 1, line: 1 }
            },
            tag
        )
    }

    #[test]
    fn parse_tag_set_2() {
        let str = "host:[a1  ,a2,a3]";

        let (_, tag) = ParsedTag::parse_from_raw(str).unwrap();

        assert_eq!(
            ParsedTag {
                key: "host",
                value: TagValue::Set(vec!["a1", "a2", "a3"]),
                position: Position { offset: 1, line: 1 }
            },
            tag
        )
    }

    #[test]
    fn parse_tag_set_3() {
        let str = "host:[a1  ,a2  ,           a3]";

        let (_, tag) = ParsedTag::parse_from_raw(str).unwrap();

        assert_eq!(
            ParsedTag {
                key: "host",
                value: TagValue::Set(vec!["a1", "a2", "a3"]),
                position: Position { offset: 1, line: 1 }
            },
            tag
        )
    }

    #[test]
    fn parse_tag_to_filter_node() {
        let str = "host:a1";

        let (_, tag) = ParsedTag::parse_from_raw(str).unwrap();

        assert_eq!(
            Node::Eq(Tag {
                key: "host",
                value: "a1",
            }),
            tag.into(),
        )
    }

    #[test]
    fn parse_tag_set_to_filter_node() {
        let str = "host:[a1 , a2 , a3]";

        let (_, tag) = ParsedTag::parse_from_raw(str).unwrap();

        assert_eq!(
            Node::Or(vec![
                Node::Eq(Tag {
                    key: "host",
                    value: "a1",
                }),
                Node::Eq(Tag {
                    key: "host",
                    value: "a2",
                }),
                Node::Eq(Tag {
                    key: "host",
                    value: "a3",
                }),
            ]),
            tag.into(),
        )
    }
}
