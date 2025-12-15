/// 실시간 CDC 테스트
///
/// 이 프로그램을 실행한 후, 다른 터미널에서 MySQL에 데이터를 INSERT/UPDATE/DELETE하면
/// binlog 이벤트를 실시간으로 캡처합니다.
///
/// 사용법:
/// 1. 이 프로그램 실행: `cargo run --example live_cdc_test`
/// 2. 다른 터미널에서:
///    ```sql
///    mysql -u root -prootpassword testdb
///    INSERT INTO users (name, email) VALUES ('테스트', 'test@example.com');
///    ```

use rust_mysql::cdc_engine::{CdcConfig, CdcEngine, SnapshotMode};
use rust_mysql::connection::ConnectionConfig;
use std::env;
use tokio::time::Duration;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 로깅 초기화
    tracing_subscriber::fmt::init();

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
        snapshot_mode: SnapshotMode::Never,
        include_ddl: true,
        gtid_filter: None,
    };

    info!("=== 실시간 MySQL CDC 모니터링 시작 ===");
    info!("연결: {}:{}", config.connection.hostname, config.connection.port);
    info!("");
    info!("이제 다른 터미널에서 MySQL에 데이터를 변경하세요:");
    info!("  mysql -u root -prootpassword testdb");
    info!("  INSERT INTO users (name, email) VALUES ('테스트', 'test@example.com');");
    info!("  UPDATE users SET email = 'new@example.com' WHERE name = '테스트';");
    info!("  DELETE FROM users WHERE name = '테스트';");
    info!("");

    // CDC 엔진 시작
    let mut engine = CdcEngine::new(config);
    engine.start().await?;

    // Binlog 스트리밍
    let mut binlog_rx = engine.stream_binlog().await?;
    info!("Binlog 모니터링 시작됨 (Ctrl+C로 종료)");
    info!("================================================");

    // 이벤트 수신 (무한 대기)
    let mut event_count = 0;
    loop {
        tokio::select! {
            event = binlog_rx.recv() => {
                if let Some(event) = event {
                    event_count += 1;
                    info!("\n🔔 [이벤트 #{}]", event_count);
                    info!("  작업: {:?}", event.op);
                    info!("  데이터베이스: {}", event.database);
                    info!("  테이블: {}", event.table);
                    info!("  시간: {}", event.timestamp);

                    if let Some(before) = &event.before {
                        info!("  변경 전: {:?}", before);
                    }
                    if let Some(after) = &event.after {
                        info!("  변경 후: {:?}", after);
                    }
                    if let Some(query) = &event.query {
                        info!("  쿼리: {}", query);
                    }
                } else {
                    info!("이벤트 스트림 종료");
                    break;
                }
            }
            _ = tokio::signal::ctrl_c() => {
                info!("\n\n종료 신호 수신. 프로그램을 종료합니다...");
                break;
            }
        }
    }

    info!("총 {}개의 이벤트를 수신했습니다.", event_count);
    Ok(())
}
