use nom::IResult;
use nom_locate::LocatedSpan;

#[allow(clippy::module_name_repetitions)]
pub type RawSpan<'a> = LocatedSpan<&'a str>;

pub type ParseResult<'a, T> = IResult<RawSpan<'a>, T>;

/// Implement the parse function to more easily convert a span into a sql
/// command
pub trait Parse<'a>: Sized {
    /// Parse the given span into self
    fn parse(input: RawSpan<'a>) -> ParseResult<'a, Self>;

    // Helper method to convert a raw str into a raw span and parse
    fn parse_from_raw(input: &'a str) -> ParseResult<'a, Self> {
        let i = LocatedSpan::new(input);
        Self::parse(i)
    }
}

#[derive(Clone, Default, Debug, PartialEq, Eq)]
pub struct Position {
    pub offset: usize,
    pub line: u32,
}

impl From<LocatedSpan<&str>> for Position {
    fn from(pos: LocatedSpan<&str>) -> Self {
        Self {
            offset: pos.get_column(),
            line: pos.location_line(),
        }
    }
}
