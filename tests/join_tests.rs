//! JOIN integration tests.

mod common;

use common::create_test_connection;

#[test]
fn test_inner_join() {
    let conn = create_test_connection();
    let results = conn
        .query("SELECT u.name, o.amount FROM users u INNER JOIN orders o ON u.id = o.user_id")
        .unwrap();
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 6);
}

#[test]
fn test_cross_join_empty_table() {
    let conn = blaze::Connection::in_memory().unwrap();
    conn.execute("CREATE TABLE t1 (a BIGINT)").unwrap();
    conn.execute("CREATE TABLE t2 (b BIGINT)").unwrap();
    conn.execute("INSERT INTO t2 VALUES (1)").unwrap();
    let results = conn.query("SELECT * FROM t1 CROSS JOIN t2").unwrap();
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 0);
}
