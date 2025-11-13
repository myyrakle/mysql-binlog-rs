//! MySQL CDC 엔진 - 스냅샷 및 스트리밍 처리
//!
//! CDC 엔진은 다음 단계로 진행됩니다:
//! 1. 초기 스냅샷 (모든 행 읽기)
//! 2. Binlog 스트리밍 (이후 변경 사항 추적)
//! 3. 상태 복원 시 놓친 이벤트 처리

use crate::connection::{ConnectionConfig, MySqlConnection};
use crate::error::{CdcError, Result};
use crate::events::*;
use crate::offset::{BinlogOffset, ProcessingState, SourceInfo};
use chrono::Utc;
use std::collections::HashMap;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

/// CDC 엔진 설정
#[derive(Debug, Clone)]
pub struct CdcConfig {
    pub connection: ConnectionConfig,
    pub databases: Vec<String>,
    pub tables: Option<Vec<String>>,
    pub snapshot_mode: SnapshotMode,
    pub include_ddl: bool,
    pub gtid_filter: Option<String>,
}

/// 스냅샷 모드
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotMode {
    /// 초기 스냅샷 + 스트리밍
    Initial,
    /// 스키마만 읽기
    SchemaOnly,
    /// 스냅샷 스킵
    Never,
    /// 증분 스냅샷
    Incremental,
}

/// 테이블 메타데이터 캐시
#[derive(Debug, Clone)]
struct TableMetadata {
    database: String,
    table: String,
    columns: Vec<String>,
    column_types: Vec<String>,
    primary_key: Vec<String>,
}

/// CDC 이벤트 수신자
pub type CdcEventReceiver = mpsc::UnboundedReceiver<ChangeEvent>;

/// MySQL CDC 엔진
pub struct CdcEngine {
    config: CdcConfig,
    conn: Option<MySqlConnection>,
    offset: BinlogOffset,
    state: ProcessingState,
    table_metadata: HashMap<String, TableMetadata>,
}

impl CdcEngine {
    /// 새 CDC 엔진 생성
    pub fn new(config: CdcConfig) -> Self {
        let offset = BinlogOffset::new("mysql-bin.000001".to_string());
        CdcEngine {
            config,
            conn: None,
            offset,
            state: ProcessingState::Snapshotting,
            table_metadata: HashMap::new(),
        }
    }

    /// 엔진 초기화 및 연결
    pub async fn start(&mut self) -> Result<()> {
        info!("Starting CDC Engine");

        // MySQL 연결
        let mut conn = MySqlConnection::connect(self.config.connection.clone()).await?;

        // 서버 정보 확인
        let server_id = conn.get_server_id().await?;
        info!("Connected to MySQL server: {}", server_id);

        // GTID 모드 확인
        let gtid_enabled = conn.is_gtid_mode_enabled().await?;
        info!("GTID mode enabled: {}", gtid_enabled);

        // Binlog 상태 확인
        let binlog_status = conn.get_binlog_status().await?;
        info!(
            "Current binlog: {} at position {}",
            binlog_status.file, binlog_status.position
        );

        self.offset
            .update_position(binlog_status.file, binlog_status.position);
        self.offset.gtid_set = binlog_status.executed_gtid_set;

        // 테이블 메타데이터 로드
        self.load_table_metadata(&mut conn).await?;

        self.conn = Some(conn);
        self.state = ProcessingState::Streaming;

        info!("CDC Engine started successfully");
        Ok(())
    }

    /// 테이블 메타데이터 로드
    async fn load_table_metadata(&mut self, conn: &mut MySqlConnection) -> Result<()> {
        info!(
            "Loading table metadata for databases: {:?}",
            self.config.databases
        );

        for database in &self.config.databases {
            // 데이터베이스에서 테이블 조회
            let tables = match conn.get_tables(database).await {
                Ok(t) => t,
                Err(e) => {
                    warn!("Failed to get tables from {}: {}", database, e);
                    continue;
                }
            };

            for table in tables {
                // 필터링 적용
                if let Some(ref table_filter) = self.config.tables {
                    if !table_filter.contains(&table) {
                        continue;
                    }
                }

                // 컬럼 정보 조회
                match conn.get_table_schema(database, &table).await {
                    Ok(columns) => {
                        let column_names: Vec<String> =
                            columns.iter().map(|c| c.name.clone()).collect();
                        let column_types: Vec<String> =
                            columns.iter().map(|c| c.column_type.clone()).collect();
                        let primary_key: Vec<String> = columns
                            .iter()
                            .filter(|c| c.is_key)
                            .map(|c| c.name.clone())
                            .collect();

                        let table_key = format!("{}.{}", database, table);
                        self.table_metadata.insert(
                            table_key,
                            TableMetadata {
                                database: database.clone(),
                                table: table.clone(),
                                columns: column_names,
                                column_types,
                                primary_key,
                            },
                        );

                        debug!("Loaded metadata for {}.{}", database, table);
                    }
                    Err(e) => {
                        warn!("Failed to get schema for {}.{}: {}", database, table, e);
                    }
                }
            }
        }

        info!("Loaded metadata for {} tables", self.table_metadata.len());
        Ok(())
    }

    /// 스냅샷 처리 (초기 데이터 읽기)
    pub async fn snapshot(&mut self) -> Result<CdcEventReceiver> {
        let (tx, rx) = mpsc::unbounded_channel();

        if self.config.snapshot_mode == SnapshotMode::Never {
            info!("Snapshot mode is NEVER, skipping snapshot");
            return Ok(rx);
        }

        info!("Starting snapshot");

        for metadata in self.table_metadata.values() {
            let query = format!("SELECT * FROM `{}`.`{}`", metadata.database, metadata.table);

            // 실제 구현에서는 MySQL 쿼리 실행하여 행 읽기
            debug!("Snapshot query: {}", query);

            // 각 행에 대해 ChangeEvent 생성
            // 생략: 실제 행 데이터는 쿼리 결과에서 읽음

            info!(
                "Snapshot complete for {}.{}",
                metadata.database, metadata.table
            );
        }

        Ok(rx)
    }

    /// Binlog 스트리밍 시작
    pub async fn stream_binlog(&mut self) -> Result<CdcEventReceiver> {
        let (tx, rx) = mpsc::unbounded_channel();

        info!(
            "Starting binlog streaming from {}",
            self.offset.binlog_position
        );

        // 실제 MySQL 프로토콜 기반 Binlog 클라이언트 필요
        // 여기서는 간단한 시뮬레이션

        Ok(rx)
    }

    /// 테이블 맵 이벤트 처리
    #[allow(dead_code)]
    fn handle_table_map(&mut self, data: &TableMapData) {
        debug!("Table map event: {}.{}", data.database, data.table);
        // 메타데이터 업데이트
    }

    /// WRITE_ROWS 이벤트를 ChangeEvent로 변환
    #[allow(dead_code)]
    fn write_rows_to_change_event(
        &self,
        data: &WriteRowsData,
        table: &TableMetadata,
    ) -> Vec<ChangeEvent> {
        data.rows
            .iter()
            .map(|row| {
                let mut after = HashMap::new();
                for (i, col_name) in table.columns.iter().enumerate() {
                    if i < row.len() {
                        after.insert(col_name.clone(), row[i].clone());
                    }
                }

                ChangeEvent {
                    gtid: None,
                    op: OperationType::Insert,
                    timestamp: Utc::now(),
                    database: table.database.clone(),
                    table: table.table.clone(),
                    before: None,
                    after: Some(after),
                    query: None,
                }
            })
            .collect()
    }

    /// UPDATE_ROWS 이벤트를 ChangeEvent로 변환
    #[allow(dead_code)]
    fn update_rows_to_change_event(
        &self,
        data: &UpdateRowsData,
        table: &TableMetadata,
    ) -> Vec<ChangeEvent> {
        data.rows
            .iter()
            .map(|(before_row, after_row)| {
                let mut before = HashMap::new();
                let mut after = HashMap::new();

                for (i, col_name) in table.columns.iter().enumerate() {
                    if i < before_row.len() {
                        before.insert(col_name.clone(), before_row[i].clone());
                    }
                    if i < after_row.len() {
                        after.insert(col_name.clone(), after_row[i].clone());
                    }
                }

                ChangeEvent {
                    gtid: None,
                    op: OperationType::Update,
                    timestamp: Utc::now(),
                    database: table.database.clone(),
                    table: table.table.clone(),
                    before: Some(before),
                    after: Some(after),
                    query: None,
                }
            })
            .collect()
    }

    /// DELETE_ROWS 이벤트를 ChangeEvent로 변환
    #[allow(dead_code)]
    fn delete_rows_to_change_event(
        &self,
        data: &DeleteRowsData,
        table: &TableMetadata,
    ) -> Vec<ChangeEvent> {
        data.rows
            .iter()
            .map(|row| {
                let mut before = HashMap::new();
                for (i, col_name) in table.columns.iter().enumerate() {
                    if i < row.len() {
                        before.insert(col_name.clone(), row[i].clone());
                    }
                }

                ChangeEvent {
                    gtid: None,
                    op: OperationType::Delete,
                    timestamp: Utc::now(),
                    database: table.database.clone(),
                    table: table.table.clone(),
                    before: Some(before),
                    after: None,
                    query: None,
                }
            })
            .collect()
    }

    /// 쿼리 이벤트를 ChangeEvent로 변환 (DDL)
    #[allow(dead_code)]
    fn query_to_change_event(&self, data: &QueryEventData) -> Option<ChangeEvent> {
        if !self.config.include_ddl {
            return None;
        }

        // DDL 쿼리 감지
        let upper_query = data.query.to_uppercase();
        if upper_query.starts_with("CREATE")
            || upper_query.starts_with("ALTER")
            || upper_query.starts_with("DROP")
        {
            return Some(ChangeEvent {
                gtid: None,
                op: OperationType::Ddl,
                timestamp: Utc::now(),
                database: data.database.clone(),
                table: String::new(),
                before: None,
                after: None,
                query: Some(data.query.clone()),
            });
        }

        None
    }

    /// 현재 오프셋 반환
    pub fn get_offset(&self) -> &BinlogOffset {
        &self.offset
    }

    /// 오프셋 저장
    pub fn save_offset(&mut self) -> Result<()> {
        // 실제 구현에서는 Kafka/파일 등에 저장
        debug!("Saving offset: {}", self.offset.binlog_position);
        Ok(())
    }

    /// 상태 조회
    pub fn get_state(&self) -> ProcessingState {
        self.state
    }

    /// 엔진 종료
    pub async fn stop(&mut self) -> Result<()> {
        info!("Stopping CDC Engine");

        if let Some(mut conn) = self.conn.take() {
            conn.close().await?;
        }

        self.state = ProcessingState::Stopped;
        info!("CDC Engine stopped");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cdc_config() {
        let config = CdcConfig {
            connection: ConnectionConfig::default(),
            databases: vec!["test".to_string()],
            tables: None,
            snapshot_mode: SnapshotMode::Initial,
            include_ddl: true,
            gtid_filter: None,
        };

        let engine = CdcEngine::new(config);
        assert_eq!(engine.state, ProcessingState::Snapshotting);
    }
}
