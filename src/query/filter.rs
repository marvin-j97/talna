use crate::query::lexer::{self, tokenize_filter_query};
use crate::smap::SeriesMapping;
use crate::{tag_index::TagIndex, SeriesId};
use std::collections::VecDeque;

#[derive(Debug, Eq, PartialEq)]
pub struct Tag<'a> {
    pub key: &'a str,
    pub value: &'a str,
}

#[derive(Debug, Eq, PartialEq)]
pub enum Node<'a> {
    And(Vec<Self>),
    Or(Vec<Self>),
    Eq(Tag<'a>),
    Not(Box<Self>),
    AllStar,
}

impl<'a> std::fmt::Display for Node<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Node::Eq(leaf) => write!(f, "{}:{}", leaf.key, leaf.value),
            Node::And(nodes) => write!(
                f,
                "({})",
                nodes
                    .iter()
                    .map(|x| x.to_string())
                    .collect::<Vec<_>>()
                    .join(" AND ")
            ),
            Node::Or(nodes) => write!(
                f,
                "({})",
                nodes
                    .iter()
                    .map(|x| x.to_string())
                    .collect::<Vec<_>>()
                    .join(" OR ")
            ),
            Node::AllStar => write!(f, "*"),
            Node::Not(node) => write!(f, "!({node})",),
        }
    }
}

pub fn intersection(vecs: &[Vec<u64>]) -> Vec<u64> {
    if vecs.is_empty() {
        return vec![];
    }

    if vecs.iter().any(Vec::is_empty) {
        return vec![];
    }

    let first_vec = &vecs[0];
    let mut result = Vec::new();

    'outer: for &elem in first_vec {
        for vec in &vecs[1..] {
            if !vec.contains(&elem) {
                continue 'outer;
            }
        }

        result.push(elem);
    }

    result
}

pub fn union(vecs: &[Vec<u64>]) -> Vec<u64> {
    let mut result = vec![];

    for vec in vecs {
        result.extend(vec)
    }

    result.sort();
    result.dedup();

    result
}

impl<'a> Node<'a> {
    // TODO: unit test and add benchmark case
    pub fn evaluate(
        &self,
        smap: &SeriesMapping,
        tag_index: &TagIndex,
        metric_name: &str,
    ) -> fjall::Result<Vec<SeriesId>> {
        match self {
            Node::AllStar => tag_index.query_eq(metric_name),
            Node::Eq(leaf) => {
                let term = format!("{metric_name}#{}:{}", leaf.key, leaf.value);
                tag_index.query_eq(&term)
            }
            Node::And(children) => {
                // TODO: evaluate lazily...
                let ids = children
                    .iter()
                    .map(|c| Self::evaluate(c, smap, tag_index, metric_name))
                    .collect::<fjall::Result<Vec<_>>>()?;

                Ok(intersection(&ids))
            }
            Node::Or(children) => {
                let ids = children
                    .iter()
                    .map(|c| Self::evaluate(c, smap, tag_index, metric_name))
                    .collect::<fjall::Result<Vec<_>>>()?;

                Ok(union(&ids))
            }
            Node::Not(node) => {
                let mut ids = smap.list_all()?;

                for id in node.evaluate(smap, tag_index, metric_name)? {
                    ids.remove(&id);
                }

                let mut ids = ids.into_iter().collect::<Vec<_>>();
                ids.sort();

                Ok(ids)
            }
        }
    }
}

#[derive(Debug)]
pub enum Item<'a> {
    Identifier((&'a str, &'a str)),
    And,
    Or,
    Not,
    ParanOpen,
    ParanClose,
}

pub fn parse_filter_query(s: &str) -> Result<Node, ()> {
    if s.trim() == "*" {
        return Ok(Node::AllStar);
    }

    let mut output_queue = VecDeque::new();
    let mut op_stack = VecDeque::new();

    for tok in tokenize_filter_query(s) {
        let tok = tok?;

        match tok {
            lexer::Token::Identifier(id) => {
                let mut splits = id.split(':');
                let k = splits.next().unwrap();
                let v = splits.next().unwrap();
                output_queue.push_back(Item::Identifier((k, v)));
            }
            lexer::Token::And => {
                loop {
                    let Some(top) = op_stack.back() else {
                        break;
                    };

                    // And has higher precedence than Or but lower than Not
                    if matches!(top, Item::And | Item::Not) {
                        output_queue.push_back(op_stack.pop_back().unwrap());
                    } else {
                        break;
                    }
                }
                op_stack.push_back(Item::And);
            }
            lexer::Token::Or => {
                loop {
                    let Some(top) = op_stack.back() else {
                        break;
                    };

                    // Or has lower precedence, so we pop And and Not operators
                    if matches!(top, Item::And | Item::Not) {
                        output_queue.push_back(op_stack.pop_back().unwrap());
                    } else {
                        break;
                    }
                }

                op_stack.push_back(Item::Or);
            }
            lexer::Token::Not => {
                op_stack.push_back(Item::Not);
            }
            lexer::Token::ParanOpen => {
                op_stack.push_back(Item::ParanOpen);
            }
            lexer::Token::ParanClose => {
                loop {
                    let Some(top) = op_stack.back() else {
                        break;
                    };

                    if matches!(top, Item::ParanOpen) {
                        break;
                    }

                    output_queue.push_back(op_stack.pop_back().unwrap());
                }

                let Some(top) = op_stack.pop_back() else {
                    return Err(());
                };

                if !matches!(top, Item::ParanOpen) {
                    return Err(());
                }
            }
        }
    }

    while let Some(top) = op_stack.pop_back() {
        if matches!(top, Item::ParanOpen) {
            return Err(());
        }
        output_queue.push_back(top);
    }

    let mut buf: Vec<Node> = Vec::new();

    for item in output_queue {
        match item {
            Item::Identifier((key, value)) => {
                buf.push(Node::Eq(Tag { key, value }));
            }
            Item::And => {
                let b = buf.pop().unwrap();
                let a = buf.pop().unwrap();
                buf.push(Node::And(vec![a, b]));
            }
            Item::Or => {
                let b = buf.pop().unwrap();
                let a = buf.pop().unwrap();
                buf.push(Node::Or(vec![a, b]));
            }
            Item::Not => {
                let a = buf.pop().unwrap();
                buf.push(Node::Not(Box::new(a)));
            }
            Item::ParanOpen => return Err(()),
            Item::ParanClose => return Err(()),
        }
    }

    debug_assert_eq!(1, buf.len());

    Ok(buf.pop().unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test_log::test]
    fn test_parse_filter_query_1() {
        assert_eq!(
            Ok(Node::Eq(Tag {
                key: "hello",
                value: "world"
            })),
            parse_filter_query("hello:world")
        );
    }

    #[test_log::test]
    fn test_parse_filter_query_2() {
        assert_eq!(
            Ok(Node::Not(Box::new(Node::Eq(Tag {
                key: "hello",
                value: "world"
            })))),
            parse_filter_query("!hello:world")
        );
    }

    #[test_log::test]
    fn test_parse_filter_query_3() {
        assert_eq!(
            Ok(Node::Not(Box::new(Node::Or(vec![
                Node::Eq(Tag {
                    key: "hello",
                    value: "world"
                }),
                Node::Eq(Tag {
                    key: "hallo",
                    value: "welt"
                }),
            ])))),
            parse_filter_query("!(hello:world OR hallo:welt)")
        );
    }

    #[test_log::test]
    fn test_intersection() {
        assert_eq!(
            [1, 3],
            *intersection(&[vec![1, 2, 3, 4, 5], vec![1, 3, 5], vec![1, 3]]),
        );
    }

    #[test_log::test]
    fn test_union() {
        assert_eq!(
            [1, 2, 4, 8],
            *union(&[vec![1, 8], vec![1, 2], vec![1, 2, 4], vec![2, 4, 8]]),
        );
    }
}
