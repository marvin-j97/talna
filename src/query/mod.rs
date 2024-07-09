pub mod filter;
pub mod lexer;

use filter::{EqLeaf, Node as FilterNode};
use lexer::tokenize_filter_query;
use std::collections::VecDeque;

#[derive(Debug)]
pub enum Item {
    Identifier((String, String)),
    And,
    Or,
    ParanOpen,
    ParanClose,
}

// TODO: NOT

pub fn parse_filter_query(s: &str) -> Result<FilterNode, ()> {
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

    let mut buf: Vec<FilterNode> = Vec::new();

    for item in output_queue {
        match item {
            Item::Identifier((key, value)) => {
                buf.push(FilterNode::Eq(EqLeaf { key, value }));
            }
            Item::And => {
                let left = buf.pop().unwrap();
                let right = buf.pop().unwrap();
                buf.push(FilterNode::And(vec![left, right]));
            }
            Item::Or => {
                let left = buf.pop().unwrap();
                let right = buf.pop().unwrap();
                buf.push(FilterNode::Or(vec![left, right]));
            }
            Item::ParanOpen => return Err(()),
            Item::ParanClose => return Err(()),
        }
    }

    debug_assert_eq!(1, buf.len());

    Ok(buf.pop().unwrap())
}
