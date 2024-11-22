use logos::Logos;

#[derive(Logos, Debug, PartialEq, Eq)]
#[logos(skip r"[ \r\t\n\f]+")] // Ignore this regex pattern between tokens
pub enum Token<'a> {
    #[token("!")]
    Not,

    #[token("AND")]
    And,

    #[token("OR")]
    Or,

    #[token("(")]
    ParanOpen,

    #[token(")")]
    ParanClose,

    #[regex("[a-zA-Z_-]+:[a-zA-Z0-9_\\-.]*\\*")]
    Wildcard(&'a str),

    #[regex("[a-zA-Z_-]+:[a-zA-Z0-9_\\-.]+")]
    Identifier(&'a str),
}

// TODO: 1.0.0 replace with nom parser

// TODO: 1.0.0 TagSet values should probably also be allowed to be integers
// so we can something like: give me the AVG response time of all 4xx HTTP responses

pub fn tokenize_filter_query(s: &str) -> impl Iterator<Item = Result<Token, ()>> + '_ {
    Token::lexer(s)
}
