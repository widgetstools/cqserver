//! Smoke test isolating whether the DataFusion basics work end-to-end.
//! If this passes, any failures in the larger harness are in our
//! adapter code; if this fails, the DataFusion install itself is
//! broken on this host.

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::execution::context::SessionContext;

#[tokio::test]
async fn datafusion_minimal_insert_and_query() {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["Alice", "Bob"])),
        ],
    )
    .expect("build batch");

    let ctx = SessionContext::new();
    let table = MemTable::try_new(schema, vec![vec![batch]]).expect("memtable");
    ctx.register_table("t", Arc::new(table)).expect("register");

    let df = ctx
        .sql("SELECT id, name FROM t WHERE id = 1")
        .await
        .expect("plan");
    let batches = df.collect().await.expect("collect");

    assert_eq!(batches.len(), 1);
    let b = &batches[0];
    assert_eq!(b.num_rows(), 1);
    let id_arr = b
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id col");
    let name_arr = b
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name col");
    assert_eq!(id_arr.value(0), 1);
    assert_eq!(name_arr.value(0), "Alice");
}
