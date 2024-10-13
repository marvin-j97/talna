use logos::Logos;

#[derive(Logos, Debug, PartialEq)]
#[logos(skip r"[ \r\t\n\f]+")] // Ignore this regex pattern between tokens
pub enum Token<'a> {
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
    let lexer = Token::lexer(s);
    lexer.into_iter()
}
