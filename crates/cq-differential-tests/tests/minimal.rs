//! Smoke test isolating whether the duckdb-rs basics work end-to-end.
//! If this passes, the issue is in our harness; if it fails, the
//! duckdb 1.10503.1 install itself isn't working on this host.

use duckdb::Connection;

#[test]
fn duckdb_minimal_insert_and_query() {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute("CREATE TABLE t (id BIGINT, name VARCHAR)", [])
        .expect("create");
    conn.execute("INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob')", [])
        .expect("insert literal");

    let mut stmt = conn.prepare("SELECT id, name FROM t WHERE id = 1").expect("prep");
    let mut rows = stmt.query([]).expect("exec");
    let row = rows.next().expect("next").expect("first row");
    let id: i64 = row.get(0).expect("id");
    let name: String = row.get(1).expect("name");
    assert_eq!(id, 1);
    assert_eq!(name, "Alice");
}
