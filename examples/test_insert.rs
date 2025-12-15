/// MySQL CDC 테스트: 데이터 INSERT 후 변경사항 캡처
///
/// 이 예제는 MySQL에 데이터를 insert하고 CDC로 변경사항을 실시간으로 캡처합니다.

use rust_mysql::cdc_engine::{CdcConfig, CdcEngine, SnapshotMode};
use rust_mysql::connection::ConnectionConfig;
use std::env;
use std::time::Duration;
use tokio::time::sleep;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 로깅 초기화
    tracing_subscriber::fmt::init();

    // 연결 설정
    let config = CdcConfig {
        connection: ConnectionConfig {
            hostname: env::var("DB_HOST").unwrap_or_else(|_| "localhost".to_string()),
            port: env::var("DB_PORT")
                .unwrap_or_else(|_| "3306".to_string())
                .parse()
                .unwrap_or(3306),
            username: env::var("DB_USER").unwrap_or_else(|_| "root".to_string()),
            password: env::var("DB_PASSWORD").unwrap_or_else(|_| "rootpassword".to_string()),
            database: Some(env::var("DB_NAME").unwrap_or_else(|_| "testdb".to_string())),
            server_id: 1,
            timeout: Duration::from_secs(30),
        },
        databases: vec!["testdb".to_string()],
        tables: None,
        snapshot_mode: SnapshotMode::Never, // 스냅샷 스킵, 실시간만 모니터링
        include_ddl: true,
        gtid_filter: None,
    };

    info!("=== MySQL CDC 테스트 시작 ===");
    info!(
        "연결 대상: {}:{}",
        config.connection.hostname, config.connection.port
    );

    // 테스트 테이블 생성 및 데이터 insert를 위한 별도 연결
    let insert_config = config.connection.clone();

    // CDC 엔진 시작
    let mut engine = CdcEngine::new(config);
    engine.start().await?;
    info!("CDC Engine 시작 완료");

    // Binlog 스트리밍 시작
    let mut binlog_rx = engine.stream_binlog().await?;
    info!("Binlog 스트리밍 시작");

    // 백그라운드에서 데이터 insert 작업 실행
    tokio::spawn(async move {
        sleep(Duration::from_secs(2)).await;

        info!("\n=== 테스트 데이터 INSERT 시작 ===");

        match insert_test_data(insert_config).await {
            Ok(_) => info!("테스트 데이터 INSERT 완료"),
            Err(e) => eprintln!("데이터 INSERT 실패: {}", e),
        }
    });

    // 변경 이벤트 수신 (10초 동안)
    info!("\n=== 변경 이벤트 수신 대기 중... ===");

    let timeout = tokio::time::timeout(Duration::from_secs(15), async {
        let mut event_count = 0;
        while let Some(event) = binlog_rx.recv().await {
            event_count += 1;
            info!("\n[이벤트 #{}]", event_count);
            info!("  작업: {:?}", event.op);
            info!("  데이터베이스: {}", event.database);
            info!("  테이블: {}", event.table);
            info!("  타임스탬프: {}", event.timestamp);

            if let Some(before) = &event.before {
                info!("  이전 값: {:?}", before);
            }
            if let Some(after) = &event.after {
                info!("  이후 값: {:?}", after);
            }
            if let Some(query) = &event.query {
                info!("  쿼리: {}", query);
            }
        }
    });

    match timeout.await {
        Ok(_) => info!("\n이벤트 스트림 종료"),
        Err(_) => info!("\n타임아웃 (15초)"),
    }

    info!("\n=== 테스트 완료 ===");
    Ok(())
}

/// 테스트 데이터 INSERT
async fn insert_test_data(config: ConnectionConfig) -> Result<(), Box<dyn std::error::Error>> {
    use mysql_async::prelude::*;
    use mysql_async::{Opts, OptsBuilder};

    // 연결 문자열 생성
    let connection_string = if let Some(ref db) = config.database {
        format!(
            "mysql://{}:{}@{}:{}/{}",
            config.username, config.password, config.hostname, config.port, db
        )
    } else {
        format!(
            "mysql://{}:{}@{}:{}",
            config.username, config.password, config.hostname, config.port
        )
    };

    let opts: Opts = connection_string.parse()?;
    let pool = mysql_async::Pool::new(opts);
    let mut conn = pool.get_conn().await?;

    // 테스트 테이블 생성
    info!("테스트 테이블 생성 중...");
    conn.query_drop(
        "CREATE TABLE IF NOT EXISTS users (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            email VARCHAR(100) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )"
    )
    .await?;

    info!("기존 데이터 삭제...");
    conn.query_drop("TRUNCATE TABLE users").await?;

    // 데이터 INSERT
    info!("데이터 INSERT 중...");

    sleep(Duration::from_millis(500)).await;
    conn.query_drop("INSERT INTO users (name, email) VALUES ('홍길동', 'hong@example.com')")
        .await?;
    info!("  -> 1번 사용자 추가: 홍길동");

    sleep(Duration::from_millis(500)).await;
    conn.query_drop("INSERT INTO users (name, email) VALUES ('김철수', 'kim@example.com')")
        .await?;
    info!("  -> 2번 사용자 추가: 김철수");

    sleep(Duration::from_millis(500)).await;
    conn.query_drop("INSERT INTO users (name, email) VALUES ('이영희', 'lee@example.com')")
        .await?;
    info!("  -> 3번 사용자 추가: 이영희");

    // UPDATE 테스트
    sleep(Duration::from_millis(500)).await;
    conn.query_drop("UPDATE users SET email = 'hong_new@example.com' WHERE name = '홍길동'")
        .await?;
    info!("  -> 홍길동 이메일 업데이트");

    // DELETE 테스트
    sleep(Duration::from_millis(500)).await;
    conn.query_drop("DELETE FROM users WHERE name = '김철수'")
        .await?;
    info!("  -> 김철수 삭제");

    conn.disconnect().await?;
    info!("데이터 작업 완료");

    Ok(())
}
