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
pub enum TagValue {
    Identifier(String),
    Set(Vec<String>),
}

impl TagValue {
    pub fn parse_identifier(input: RawSpan<'_>) -> ParseResult<'_, Self> {
        let (input, value) =
            take_while(|x: char| x.is_alphanumeric() || x == '_' || x == '-')(input)?;
        Ok((input, TagValue::Identifier(value.fragment().to_string())))
    }

    pub fn parse_set(input: RawSpan<'_>) -> ParseResult<'_, Self> {
        let (input, _) = tag("(")(input)?;

        let (input, values) = separated_list1(
            delimited(space0, tag(","), space0),
            take_while(|x: char| x.is_alphanumeric() || x == '_' || x == '-'),
        )(input)?;

        let (input, _) = tag(")")(input)?;

        let values = values
            .into_iter()
            .map(|x| x.fragment().to_string())
            .collect();

        Ok((input, TagValue::Set(values)))
    }
}

impl<'a> Parse<'a> for TagValue {
    fn parse(input: RawSpan<'a>) -> ParseResult<'a, Self> {
        let (input, result) = alt((Self::parse_set, Self::parse_identifier))(input)?;
        Ok((input, result))
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct ParsedTag {
    pub key: String,
    pub value: TagValue,
    position: Position,
}

impl Into<Node> for ParsedTag {
    fn into(self) -> Node {
        match self.value {
            TagValue::Identifier(value) => Node::Eq(Tag {
                key: self.key,
                value,
            }),
            TagValue::Set(vs) => Node::Or(
                vs.into_iter()
                    .map(|value| {
                        Node::Eq(Tag {
                            key: self.key.clone(),
                            value,
                        })
                    })
                    .collect(),
            ),
        }
    }
}

impl<'a> Parse<'a> for ParsedTag {
    fn parse(input: RawSpan<'a>) -> ParseResult<'a, Self> {
        let (input, pos) = position(input)?;

        let (input, key) = take_while(|x: char| x.is_alphanumeric() || x == '_')(input)?;
        let (input, _) = tag(":")(input)?;
        let (input, value) = TagValue::parse(input)?;

        Ok((
            input,
            Self {
                key: key.fragment().to_string(),
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
                key: "host".into(),
                value: TagValue::Identifier("a1".into()),
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
                key: "host_name".into(),
                value: TagValue::Identifier("t-128".into()),
                position: Position { offset: 1, line: 1 }
            },
            tag
        )
    }

    #[test]
    fn parse_tag_set() {
        let str = "host:(a1, a2, a3)";

        let (_, tag) = ParsedTag::parse_from_raw(str).unwrap();

        assert_eq!(
            ParsedTag {
                key: "host".into(),
                value: TagValue::Set(vec!["a1".into(), "a2".into(), "a3".into()]),
                position: Position { offset: 1, line: 1 }
            },
            tag
        )
    }

    #[test]
    fn parse_tag_set_2() {
        let str = "host:(a1  ,a2,a3)";

        let (_, tag) = ParsedTag::parse_from_raw(str).unwrap();

        assert_eq!(
            ParsedTag {
                key: "host".into(),
                value: TagValue::Set(vec!["a1".into(), "a2".into(), "a3".into()]),
                position: Position { offset: 1, line: 1 }
            },
            tag
        )
    }

    #[test]
    fn parse_tag_set_3() {
        let str = "host:(a1  ,a2  ,           a3)";

        let (_, tag) = ParsedTag::parse_from_raw(str).unwrap();

        assert_eq!(
            ParsedTag {
                key: "host".into(),
                value: TagValue::Set(vec!["a1".into(), "a2".into(), "a3".into()]),
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
                key: "host".into(),
                value: "a1".into()
            }),
            tag.into(),
        )
    }

    #[test]
    fn parse_tag_set_to_filter_node() {
        let str = "host:(a1 , a2 , a3)";

        let (_, tag) = ParsedTag::parse_from_raw(str).unwrap();

        assert_eq!(
            Node::Or(vec![
                Node::Eq(Tag {
                    key: "host".into(),
                    value: "a1".into()
                }),
                Node::Eq(Tag {
                    key: "host".into(),
                    value: "a2".into()
                }),
                Node::Eq(Tag {
                    key: "host".into(),
                    value: "a3".into()
                }),
            ]),
            tag.into(),
        )
    }
}
