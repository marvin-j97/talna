use super::{
    span::{Parse, ParseResult, Position, RawSpan},
    tag_value::TagValue,
};
use nom::bytes::complete::{tag, take_while};
use nom_locate::position;

#[derive(Debug, Eq, PartialEq)]
pub struct ParsedTag<'a> {
    pub key: &'a str,
    pub value: TagValue<'a>,
    position: Position,
}

/* impl<'a> From<ParsedTag<'a>> for Node<'a> {
    fn from(val: ParsedTag<'a>) -> Self {
        match val.value {
            TagValue::String(value) => Node::Eq(Tag {
                key: val.key,
                value,
            }),
            TagValue::Set(vs) => Node::Or(
                vs.into_iter()
                    .map(|value| {
                        todo!()

                        /*   Node::Eq(Tag {
                            key: val.key,
                            value,
                        }) */
                    })
                    .collect(),
            ),
            _ => unimplemented!(),
        }
    }
} */

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
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::ops::Bound::{Excluded, Included, Unbounded};
    use test_log::test;

    #[test]
    fn parse_full_range() {
        let str = "host:[..]";

        let (_, tag) = ParsedTag::parse_from_raw(str).unwrap();

        assert_eq!(
            ParsedTag {
                key: "host",
                value: TagValue::IntegerRange((Unbounded, Unbounded)),
                position: Position { offset: 1, line: 1 }
            },
            tag
        );
    }

    #[test]
    fn parse_half_open_range() {
        let str = "host:[5..]";

        let (_, tag) = ParsedTag::parse_from_raw(str).unwrap();

        assert_eq!(
            ParsedTag {
                key: "host",
                value: TagValue::IntegerRange((Included(5), Unbounded)),
                position: Position { offset: 1, line: 1 }
            },
            tag
        );
    }

    #[test]
    fn parse_half_open_range_2() {
        let str = "host:[983..]";

        let (_, tag) = ParsedTag::parse_from_raw(str).unwrap();

        assert_eq!(
            ParsedTag {
                key: "host",
                value: TagValue::IntegerRange((Included(983), Unbounded)),
                position: Position { offset: 1, line: 1 }
            },
            tag
        );
    }

    #[test]
    fn parse_closed_range() {
        let str = "host:[400..500]";

        let (_, tag) = ParsedTag::parse_from_raw(str).unwrap();

        assert_eq!(
            ParsedTag {
                key: "host",
                value: TagValue::IntegerRange((Included(400), Excluded(500))),
                position: Position { offset: 1, line: 1 }
            },
            tag
        );
    }

    #[test]
    fn parse_closed_range_inclusive() {
        let str = "host:[400..=500]";

        let (_, tag) = ParsedTag::parse_from_raw(str).unwrap();

        assert_eq!(
            ParsedTag {
                key: "host",
                value: TagValue::IntegerRange((Included(400), Included(500))),
                position: Position { offset: 1, line: 1 }
            },
            tag
        );
    }

    #[test]
    fn parse_tag_simple() {
        let str = "host:a1";

        let (_, tag) = ParsedTag::parse_from_raw(str).unwrap();

        assert_eq!(
            ParsedTag {
                key: "host",
                value: TagValue::String("a1"),
                position: Position { offset: 1, line: 1 }
            },
            tag
        );
    }

    #[test]
    fn parse_tag_simple_2() {
        let str = "host_name:t-128";

        let (_, tag) = ParsedTag::parse_from_raw(str).unwrap();

        assert_eq!(
            ParsedTag {
                key: "host_name",
                value: TagValue::String("t-128"),
                position: Position { offset: 1, line: 1 }
            },
            tag
        );
    }

    #[test]
    fn parse_tag_simple_3() {
        let str = "host_name:t_128";

        let (_, tag) = ParsedTag::parse_from_raw(str).unwrap();

        assert_eq!(
            ParsedTag {
                key: "host_name",
                value: TagValue::String("t_128"),
                position: Position { offset: 1, line: 1 }
            },
            tag
        );
    }

    #[test]
    fn parse_tag_simple_int() {
        let str = "host:123";

        let (_, tag) = ParsedTag::parse_from_raw(str).unwrap();

        assert_eq!(
            ParsedTag {
                key: "host",
                value: TagValue::Integer(123),
                position: Position { offset: 1, line: 1 }
            },
            tag
        );
    }

    #[test]
    fn parse_tag_str_conflict() {
        let str = "host:2015_test";

        let (_, tag) = ParsedTag::parse_from_raw(str).unwrap();

        assert_eq!(
            ParsedTag {
                key: "host",
                value: TagValue::String("2015_test"),
                position: Position { offset: 1, line: 1 }
            },
            tag
        );
    }

    #[test]
    fn parse_tag_set() {
        let str = "host:[a1, a2, a3]";

        let (_, tag) = ParsedTag::parse_from_raw(str).unwrap();

        assert_eq!(
            ParsedTag {
                key: "host",
                value: TagValue::Set(vec![
                    TagValue::String("a1"),
                    TagValue::String("a2"),
                    TagValue::String("a3")
                ]),
                position: Position { offset: 1, line: 1 }
            },
            tag
        );
    }

    #[test]
    fn parse_tag_set_2() {
        let str = "host:[a1  ,a2,a3]";

        let (_, tag) = ParsedTag::parse_from_raw(str).unwrap();

        assert_eq!(
            ParsedTag {
                key: "host",
                value: TagValue::Set(vec![
                    TagValue::String("a1"),
                    TagValue::String("a2"),
                    TagValue::String("a3")
                ]),
                position: Position { offset: 1, line: 1 }
            },
            tag
        );
    }

    #[test]
    fn parse_tag_set_3() {
        let str = "host:[a1  ,a2  ,           a3]";

        let (_, tag) = ParsedTag::parse_from_raw(str).unwrap();

        assert_eq!(
            ParsedTag {
                key: "host",
                value: TagValue::Set(vec![
                    TagValue::String("a1"),
                    TagValue::String("a2"),
                    TagValue::String("a3")
                ]),
                position: Position { offset: 1, line: 1 }
            },
            tag
        );
    }

    // TODO: move to Filter AST tests
    /*  #[test]
    fn parse_tag_to_filter_node() {
        let str = "host:a1";

        let (_, tag) = ParsedTag::parse_from_raw(str).unwrap();

        assert_eq!(
            Node::Eq(Tag {
                key: "host",
                value: "a1",
            }),
            tag.into(),
        );
    } */

    /*   #[test]
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
        );
    } */
}
