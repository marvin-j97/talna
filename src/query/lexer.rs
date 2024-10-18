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

    #[regex("[a-zA-Z_-]+:[a-zA-Z0-9_-]+")]
    Identifier(&'a str),
}

pub fn tokenize_filter_query(s: &str) -> impl Iterator<Item = Result<Token, ()>> + '_ {
    Token::lexer(s)
}
