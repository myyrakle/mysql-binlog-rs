//! Binlog 처리 위치 추적 (Offset 및 SourceInfo)
//!
//! Binlog 파일명 + 위치로 정확한 재시작 지점을 추적합니다.
//! 예: "mysql-bin.000003" 파일의 4097 바이트 위치

use crate::gtid::GtidSet;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Binlog 파일 위치 정보
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BinlogPosition {
    /// 바이너리 로그 파일명 (e.g., "mysql-bin.000001")
    pub filename: String,
    /// 바이트 위치
    pub position: u64,
}

impl BinlogPosition {
    pub fn new(filename: String, position: u64) -> Self {
        BinlogPosition { filename, position }
    }

    /// 파일명에서 시퀀스 번호 추출
    pub fn file_sequence(&self) -> Option<u64> {
        self.filename
            .split('.')
            .last()
            .and_then(|s| s.parse().ok())
    }
}

impl fmt::Display for BinlogPosition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.filename, self.position)
    }
}

/// Binlog 처리 오프셋 (상태 저장용)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinlogOffset {
    /// 현재 바이너리 로그 파일명과 위치
    pub binlog_position: BinlogPosition,
    /// 처리된 GTID 집합
    pub gtid_set: GtidSet,
    /// 스냅샷 완료 여부
    pub snapshot_completed: bool,
    /// 남은 이벤트 수 (스냅샷 재시작 시)
    pub events_to_skip: Option<u64>,
    /// 남은 행 수 (스냅샷 재시작 시)
    pub rows_to_skip: Option<u64>,
}

impl BinlogOffset {
    pub fn new(filename: String) -> Self {
        BinlogOffset {
            binlog_position: BinlogPosition::new(filename, 4), // MySQL binlog은 4 바이트 헤더로 시작
            gtid_set: GtidSet::new(),
            snapshot_completed: false,
            events_to_skip: None,
            rows_to_skip: None,
        }
    }

    pub fn update_position(&mut self, filename: String, position: u64) {
        self.binlog_position = BinlogPosition::new(filename, position);
    }
}

/// 현재 처리 상태 정보 (Debezium의 SourceInfo와 유사)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceInfo {
    /// MySQL 서버 ID
    pub server_id: u32,
    /// Binlog 파일명
    pub binlog_filename: String,
    /// Binlog 위치
    pub binlog_position: u64,
    /// GTID (있는 경우)
    pub gtid: Option<String>,
    /// 이벤트 타임스탬프 (초 단위)
    pub ts_sec: u32,
    /// 작은 정수 ID (순차 번호)
    pub row: Option<u64>,
    /// 스냅샷 처리 중 여부
    pub snapshot: bool,
    /// 데이터베이스명
    pub database: Option<String>,
    /// 테이블명
    pub table: Option<String>,
    /// 스레드 ID
    pub thread_id: Option<u32>,
}

impl SourceInfo {
    pub fn new(server_id: u32, binlog_filename: String) -> Self {
        SourceInfo {
            server_id,
            binlog_filename,
            binlog_position: 4,
            gtid: None,
            ts_sec: 0,
            row: None,
            snapshot: false,
            database: None,
            table: None,
            thread_id: None,
        }
    }

    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "server_id": self.server_id,
            "file": self.binlog_filename,
            "pos": self.binlog_position,
            "gtid": self.gtid,
            "ts_sec": self.ts_sec,
            "row": self.row,
            "snapshot": self.snapshot,
            "db": self.database,
            "table": self.table,
            "thread": self.thread_id,
        })
    }
}

impl fmt::Display for SourceInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SourceInfo {{ server_id: {}, file: {}, pos: {}, gtid: {:?} }}",
            self.server_id, self.binlog_filename, self.binlog_position, self.gtid
        )
    }
}

/// CDC 처리 상태
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProcessingState {
    /// 스냅샷 처리 중
    Snapshotting,
    /// 스트리밍 처리 중
    Streaming,
    /// 중단됨
    Stopped,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_binlog_position_parse() {
        let pos = BinlogPosition::new("mysql-bin.000123".to_string(), 4096);
        assert_eq!(pos.file_sequence(), Some(123));
    }

    #[test]
    fn test_source_info_json() {
        let mut info = SourceInfo::new(1, "mysql-bin.000001".to_string());
        info.database = Some("test_db".to_string());
        info.table = Some("users".to_string());
        let json = info.to_json();
        assert_eq!(json["server_id"], 1);
    }
}
