/// MySQL에 테스트 데이터 INSERT
///
/// 이 프로그램은 MySQL에 데이터를 INSERT합니다.

use mysql_async::prelude::*;
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let hostname = env::var("DB_HOST").unwrap_or_else(|_| "localhost".to_string());
    let port = env::var("DB_PORT")
        .unwrap_or_else(|_| "3306".to_string())
        .parse()
        .unwrap_or(3306);
    let username = env::var("DB_USER").unwrap_or_else(|_| "root".to_string());
    let password = env::var("DB_PASSWORD").unwrap_or_else(|_| "rootpassword".to_string());
    let database = env::var("DB_NAME").unwrap_or_else(|_| "testdb".to_string());

    let connection_string = format!(
        "mysql://{}:{}@{}:{}/{}",
        username, password, hostname, port, database
    );

    let pool = mysql_async::Pool::new(connection_string.as_str());
    let mut conn = pool.get_conn().await?;

    println!("Connected to MySQL");

    // 테이블 생성 (이미 있으면 무시)
    conn.query_drop(
        "CREATE TABLE IF NOT EXISTS users (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            email VARCHAR(100) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )"
    )
    .await?;

    println!("Table ready");

    // 데이터 INSERT
    println!("\nInserting data...");

    conn.exec_drop(
        "INSERT INTO users (name, email) VALUES (?, ?)",
        ("홍길동", "hong@example.com"),
    )
    .await?;
    println!("✅ Inserted: 홍길동");

    conn.exec_drop(
        "INSERT INTO users (name, email) VALUES (?, ?)",
        ("김철수", "kim@example.com"),
    )
    .await?;
    println!("✅ Inserted: 김철수");

    conn.exec_drop(
        "INSERT INTO users (name, email) VALUES (?, ?)",
        ("이영희", "lee@example.com"),
    )
    .await?;
    println!("✅ Inserted: 이영희");

    // UPDATE
    println!("\nUpdating data...");
    conn.exec_drop(
        "UPDATE users SET email = ? WHERE name = ?",
        ("hong_new@example.com", "홍길동"),
    )
    .await?;
    println!("✅ Updated: 홍길동");

    // DELETE
    println!("\nDeleting data...");
    conn.exec_drop(
        "DELETE FROM users WHERE name = ?",
        ("김철수",),
    )
    .await?;
    println!("✅ Deleted: 김철수");

    // 결과 확인
    println!("\nCurrent data:");
    let users: Vec<(i32, String, String)> = conn
        .query("SELECT id, name, email FROM users ORDER BY id")
        .await?;

    for (id, name, email) in users {
        println!("  {} | {} | {}", id, name, email);
    }

    println!("\nDone!");

    Ok(())
}
