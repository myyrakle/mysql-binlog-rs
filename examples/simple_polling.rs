/// MySQL 변경 감지 - 간단한 폴링 방식
///
/// 실제 binlog 프로토콜 대신 주기적으로 테이블을 조회하여 변경사항을 감지하는 예제

use mysql_async::prelude::*;
use mysql_async::{Opts, Pool};
use std::collections::HashMap;
use std::env;
use std::time::Duration;
use tokio::time::{interval, sleep};
use tracing::info;

#[derive(Debug, Clone)]
struct User {
    id: i32,
    name: String,
    email: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

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

    info!("=== MySQL 변경 감지 테스트 (폴링 방식) ===");
    info!("연결: {}:{}/{}", hostname, port, database);

    let opts: Opts = connection_string.parse()?;
    let pool = Pool::new(opts);

    // 테스트 테이블 준비
    setup_test_table(&pool).await?;

    // 변경 감지 시작 (백그라운드)
    let pool_clone = pool.clone();
    let monitor_handle = tokio::spawn(async move {
        if let Err(e) = monitor_changes(pool_clone).await {
            eprintln!("모니터링 에러: {}", e);
        }
    });

    // 메인 스레드에서 데이터 변경 작업 수행
    sleep(Duration::from_secs(2)).await;
    info!("\n=== 데이터 변경 작업 시작 ===");

    perform_changes(&pool).await?;

    // 모니터링 계속 실행 (추가 5초)
    sleep(Duration::from_secs(5)).await;

    info!("\n=== 테스트 완료 ===");
    monitor_handle.abort();

    Ok(())
}

async fn setup_test_table(pool: &Pool) -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = pool.get_conn().await?;

    info!("테스트 테이블 생성...");
    conn.query_drop(
        "CREATE TABLE IF NOT EXISTS users (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            email VARCHAR(100) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        )",
    )
    .await?;

    conn.query_drop("TRUNCATE TABLE users").await?;
    info!("테이블 준비 완료\n");

    Ok(())
}

async fn monitor_changes(pool: Pool) -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = pool.get_conn().await?;
    let mut previous_state: HashMap<i32, User> = HashMap::new();
    let mut poll_interval = interval(Duration::from_millis(500));

    info!("=== 변경 모니터링 시작 ===\n");

    loop {
        poll_interval.tick().await;

        // 현재 상태 조회
        let current_users: Vec<(i32, String, String)> = conn
            .query("SELECT id, name, email FROM users ORDER BY id")
            .await?;

        let mut current_state: HashMap<i32, User> = HashMap::new();
        for (id, name, email) in current_users {
            current_state.insert(
                id,
                User {
                    id,
                    name: name.clone(),
                    email: email.clone(),
                },
            );
        }

        // 변경 감지

        // 1. INSERT 감지 (새로운 ID)
        for (id, user) in &current_state {
            if !previous_state.contains_key(id) {
                info!("🆕 [INSERT] 새 사용자 추가:");
                info!("   ID: {}, 이름: {}, 이메일: {}", user.id, user.name, user.email);
            }
        }

        // 2. UPDATE 감지 (값 변경)
        for (id, current_user) in &current_state {
            if let Some(prev_user) = previous_state.get(id) {
                if prev_user.name != current_user.name || prev_user.email != current_user.email {
                    info!("🔄 [UPDATE] 사용자 정보 변경:");
                    info!("   ID: {}", id);
                    if prev_user.name != current_user.name {
                        info!("   이름: {} -> {}", prev_user.name, current_user.name);
                    }
                    if prev_user.email != current_user.email {
                        info!("   이메일: {} -> {}", prev_user.email, current_user.email);
                    }
                }
            }
        }

        // 3. DELETE 감지 (사라진 ID)
        for (id, user) in &previous_state {
            if !current_state.contains_key(id) {
                info!("🗑️  [DELETE] 사용자 삭제:");
                info!("   ID: {}, 이름: {}, 이메일: {}", user.id, user.name, user.email);
            }
        }

        previous_state = current_state;
    }
}

async fn perform_changes(pool: &Pool) -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = pool.get_conn().await?;

    // INSERT 테스트
    sleep(Duration::from_millis(500)).await;
    conn.query_drop("INSERT INTO users (name, email) VALUES ('홍길동', 'hong@example.com')")
        .await?;
    info!("✅ 1번 사용자 추가: 홍길동");

    sleep(Duration::from_secs(1)).await;
    conn.query_drop("INSERT INTO users (name, email) VALUES ('김철수', 'kim@example.com')")
        .await?;
    info!("✅ 2번 사용자 추가: 김철수");

    sleep(Duration::from_secs(1)).await;
    conn.query_drop("INSERT INTO users (name, email) VALUES ('이영희', 'lee@example.com')")
        .await?;
    info!("✅ 3번 사용자 추가: 이영희");

    // UPDATE 테스트
    sleep(Duration::from_secs(1)).await;
    conn.query_drop("UPDATE users SET email = 'hong_new@example.com' WHERE name = '홍길동'")
        .await?;
    info!("✅ 홍길동 이메일 업데이트");

    sleep(Duration::from_secs(1)).await;
    conn.query_drop("UPDATE users SET name = '김영수' WHERE name = '김철수'")
        .await?;
    info!("✅ 김철수 이름 변경 -> 김영수");

    // DELETE 테스트
    sleep(Duration::from_secs(1)).await;
    conn.query_drop("DELETE FROM users WHERE name = '이영희'")
        .await?;
    info!("✅ 이영희 삭제");

    sleep(Duration::from_secs(1)).await;
    conn.query_drop("INSERT INTO users (name, email) VALUES ('박지성', 'park@example.com')")
        .await?;
    info!("✅ 4번 사용자 추가: 박지성");

    info!("\n모든 변경 작업 완료");

    Ok(())
}
