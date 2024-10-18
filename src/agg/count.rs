#[derive(Clone)]
pub struct Count;

impl super::stream::Aggregation for Count {
    fn init(_: crate::Value) -> crate::Value {
        1.0
    }

    fn transform(accu: crate::Value, _: crate::Value) -> crate::Value {
        accu + 1.0
    }
}
