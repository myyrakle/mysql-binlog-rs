/// MySQL CDC 엔진 사용 예제
///
/// 이 프로그램은 MySQL 데이터베이스의 변경 데이터를 캡처하고 처리합니다.
use rust_mysql::cdc_engine::{CdcConfig, CdcEngine, SnapshotMode};
use rust_mysql::connection::ConnectionConfig;
use std::env;
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
            username: env::var("DB_USER").unwrap_or_else(|_| "testuser".to_string()),
            password: env::var("DB_PASSWORD").unwrap_or_else(|_| "testpass".to_string()),
            database: Some(env::var("DB_NAME").unwrap_or_else(|_| "testdb".to_string())),
            server_id: 1,
            timeout: std::time::Duration::from_secs(30),
        },
        databases: vec!["test".to_string()],
        tables: None,
        snapshot_mode: SnapshotMode::Initial,
        include_ddl: true,
        gtid_filter: None,
    };

    info!("Starting MySQL CDC Engine");
    info!(
        "Connecting to {}:{}",
        config.connection.hostname, config.connection.port
    );

    // CDC 엔진 생성 및 시작
    let mut engine = CdcEngine::new(config);
    engine.start().await?;

    info!("CDC Engine started successfully");

    // 스냅샷 처리
    let _snapshot_rx = engine.snapshot().await?;
    info!("Snapshot phase started");

    // Binlog 스트리밍
    let _binlog_rx = engine.stream_binlog().await?;
    info!("Binlog streaming started");

    // 변경 이벤트 처리 (간단한 예제)
    info!("Listening for change events...");

    // 실제 구현에서는:
    // - _snapshot_rx와 _binlog_rx에서 이벤트를 수신
    // - ChangeEvent를 처리 (Kafka, 파일, 데이터베이스 등으로 전송)
    // - 오프셋 관리

    Ok(())
}

/// CDC 통합 테스트 예제
#[cfg(test)]
mod integration_tests {
    use super::*;

    #[tokio::test]
    #[ignore] // 실제 MySQL 연결 필요
    async fn test_cdc_engine_initialization() -> Result<(), Box<dyn std::error::Error>> {
        let config = CdcConfig {
            connection: connection::ConnectionConfig::new("localhost", "root"),
            databases: vec!["test".to_string()],
            tables: None,
            snapshot_mode: cdc_engine::SnapshotMode::Never,
            include_ddl: false,
            gtid_filter: None,
        };

        let mut engine = CdcEngine::new(config);
        // engine.start().await?; // 실제 테스트 시 활성화

        Ok(())
    }
}
