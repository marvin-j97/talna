use crate::query::lexer::{self, tokenize_filter_query};
use crate::{tag_index::TagIndex, SeriesId};
use std::{cmp::Reverse, collections::BinaryHeap};

#[derive(Debug)]
pub enum Node {
    And(Vec<Node>),
    Or(Vec<Node>),
    Eq(EqLeaf),
}

impl std::fmt::Display for Node {
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
        }
    }
}

fn intersection(vecs: Vec<Vec<u64>>) -> Vec<u64> {
    if vecs.is_empty() {
        return vec![];
    }

    // NOTE: Short-circuit
    if vecs.iter().any(Vec::is_empty) {
        return vec![];
    }

    let mut heap = BinaryHeap::new();

    for (i, vec) in vecs.iter().enumerate() {
        if !vec.is_empty() {
            heap.push(Reverse((vec[0], i, 0)));
        }
    }

    let mut result = Vec::new();
    let mut current_min: Option<u64> = None;
    let mut count = 0;

    while let Some(Reverse((value, vec_index, elem_index))) = heap.pop() {
        if Some(value) != current_min {
            if count == vecs.len() {
                result.push(current_min.unwrap());
            }
            current_min = Some(value);
            count = 1;
        } else {
            count += 1;
        }

        let next_elem_index = elem_index + 1;
        if next_elem_index < vecs[vec_index].len() {
            heap.push(Reverse((
                vecs[vec_index][next_elem_index],
                vec_index,
                next_elem_index,
            )));
        }

        if heap.is_empty() || heap.peek().unwrap().0 .0 != value {
            if count == vecs.len() {
                result.push(value);
            }
            current_min = None;
            count = 0;
        }
    }

    result
}

fn union(vecs: Vec<Vec<u64>>) -> Vec<u64> {
    if vecs.is_empty() {
        return vec![];
    }

    let mut heap = BinaryHeap::new();

    for (i, vec) in vecs.iter().enumerate() {
        if !vec.is_empty() {
            heap.push(Reverse((vec[0], i, 0)));
        }
    }

    let mut result = Vec::new();
    let mut last_value: Option<u64> = None;

    while let Some(Reverse((value, vec_index, elem_index))) = heap.pop() {
        // Skip duplicate values
        if Some(value) != last_value {
            result.push(value);
            last_value = Some(value);
        }

        let next_elem_index = elem_index + 1;
        if next_elem_index < vecs[vec_index].len() {
            heap.push(Reverse((
                vecs[vec_index][next_elem_index],
                vec_index,
                next_elem_index,
            )));
        }
    }

    result
}

impl Node {
    pub fn evaluate(
        &self,
        tag_index: &TagIndex,
        metric_name: &str,
    ) -> fjall::Result<Vec<SeriesId>> {
        match self {
            Node::Eq(leaf) => {
                let term = format!("{metric_name}#{}:{}", leaf.key, leaf.value);
                tag_index.query(&term)
            }
            Node::And(children) => {
                // TODO: evaluate lazily...
                let ids = children
                    .iter()
                    .map(|c| Self::evaluate(c, tag_index, metric_name))
                    .collect::<fjall::Result<Vec<_>>>()?;

                Ok(intersection(ids))
            }
            Node::Or(children) => {
                let ids = children
                    .iter()
                    .map(|c| Self::evaluate(c, tag_index, metric_name))
                    .collect::<fjall::Result<Vec<_>>>()?;

                Ok(union(ids))
            }
        }
    }
}

// TODO: lifetime
#[derive(Debug)]
pub struct EqLeaf {
    pub key: String,
    pub value: String,
}

// TODO: NOT

use std::collections::VecDeque;

#[derive(Debug)]
pub enum Item {
    Identifier((String, String)),
    And,
    Or,
    ParanOpen,
    ParanClose,
}

pub fn parse_filter_query(s: &str) -> Result<Node, ()> {
    let mut output_queue = VecDeque::new();
    let mut op_stack = VecDeque::new();

    for tok in tokenize_filter_query(s) {
        let tok = tok?;

        match tok {
            lexer::Token::Identifier(id) => {
                let mut splits = id.split(':');
                let k = splits.next().unwrap().to_owned();
                let v = splits.next().unwrap().to_owned();
                output_queue.push_back(Item::Identifier((k, v)));
            }
            lexer::Token::And => {
                op_stack.push_back(Item::And);
            }
            lexer::Token::Or => {
                loop {
                    let Some(top) = op_stack.back() else {
                        break;
                    };
                    if matches!(top, Item::And) {
                        output_queue.push_back(op_stack.pop_back().unwrap());
                    } else {
                        break;
                    }
                }

                op_stack.push_back(Item::Or);
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
                    let top = op_stack.pop_back().unwrap();
                    if op_stack.is_empty() {
                        return Err(());
                    }
                    output_queue.push_back(top);
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
                buf.push(Node::Eq(EqLeaf { key, value }));
            }
            Item::And => {
                let left = buf.pop().unwrap();
                let right = buf.pop().unwrap();
                buf.push(Node::And(vec![left, right]));
            }
            Item::Or => {
                let left = buf.pop().unwrap();
                let right = buf.pop().unwrap();
                buf.push(Node::Or(vec![left, right]));
            }
            Item::ParanOpen => return Err(()),
            Item::ParanClose => return Err(()),
        }
    }

    debug_assert_eq!(1, buf.len());

    Ok(buf.pop().unwrap())
}
