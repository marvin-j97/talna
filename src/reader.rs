use crate::{db::Series, merge::StreamItem};
use self_cell::self_cell;

type BoxedMerge<'a> = Box<dyn Iterator<Item = crate::Result<StreamItem>> + 'a>;

self_cell!(
    pub struct Reader<'a> {
        owner: Vec<Series>,

        #[covariant]
        dependent: BoxedMerge,
    }
);

impl<'a> Iterator for Reader<'a> {
    type Item = crate::Result<StreamItem>;

    fn next(&mut self) -> Option<Self::Item> {
        self.with_dependent_mut(|_, iter| iter.next())
    }
}
