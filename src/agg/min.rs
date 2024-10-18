#[derive(Clone)]
pub struct Min;

impl super::stream::Aggregation for Min {
    fn transform(accu: crate::Value, x: crate::Value) -> crate::Value {
        accu.min(x)
    }
}
