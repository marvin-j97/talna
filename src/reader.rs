use fjall::Partition;
use self_cell::self_cell;

type BoxedMerge<'a> = Box<dyn Iterator<Item = fjall::Result<(u128, f32)>> + 'a>;

self_cell!(
    pub struct Reader<'a> {
        owner: Vec<Partition>,

        #[covariant]
        dependent: BoxedMerge,
    }
);

impl<'a> Iterator for Reader<'a> {
    type Item = fjall::Result<(u128, f32)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.with_dependent_mut(|_, iter| iter.next())
    }
}
