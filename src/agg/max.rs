#[derive(Clone)]
pub struct Max;

impl super::stream::Aggregation for Max {
    fn transform(accu: crate::Value, x: crate::Value) -> crate::Value {
        accu.max(x)
    }
}
