//! MySQL Binlog 이벤트 타입 및 데이터 구조 정의

use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use std::collections::HashMap;

/// MySQL Binlog 이벤트 타입
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum EventType {
    /// 알 수 없는 이벤트
    Unknown = 0,
    /// 로테이션 이벤트 (새 binlog 파일)
    RotateEvent = 4,
    /// 쿼리 이벤트 (DDL, DML)
    QueryEvent = 2,
    /// 테이블 맵 이벤트 (스키마 정보)
    TableMapEvent = 19,
    /// WRITE_ROWS 이벤트 (INSERT)
    WriteRowsEvent = 30,
    /// UPDATE_ROWS 이벤트 (UPDATE)
    UpdateRowsEvent = 31,
    /// DELETE_ROWS 이벤트 (DELETE)
    DeleteRowsEvent = 32,
    /// GTID 이벤트 (Global Transaction ID)
    GtidEvent = 33,
    /// 익명 GTID 이벤트
    AnonymousGtidEvent = 34,
    /// Rows Query 이벤트 (원본 쿼리)
    RowsQueryEvent = 36,
    /// 트랜잭션 페이로드 이벤트
    TransactionPayloadEvent = 38,
}

impl EventType {
    pub fn from_u8(val: u8) -> Self {
        match val {
            4 => EventType::RotateEvent,
            2 => EventType::QueryEvent,
            19 => EventType::TableMapEvent,
            30 => EventType::WriteRowsEvent,
            31 => EventType::UpdateRowsEvent,
            32 => EventType::DeleteRowsEvent,
            33 => EventType::GtidEvent,
            34 => EventType::AnonymousGtidEvent,
            36 => EventType::RowsQueryEvent,
            38 => EventType::TransactionPayloadEvent,
            _ => EventType::Unknown,
        }
    }
}

/// Binlog 이벤트 헤더
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventHeader {
    /// 이벤트 타입스탬프 (초 단위)
    pub timestamp: u32,
    /// 이벤트 타입
    pub event_type: EventType,
    /// MySQL 서버 ID
    pub server_id: u32,
    /// 이벤트 길이 (바이트)
    pub event_length: u32,
    /// 다음 이벤트 위치
    pub next_pos: u32,
    /// 이벤트 플래그
    pub flags: u16,
}

/// 테이블 맵 정보 (컬럼 메타데이터)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMapData {
    /// 테이블 ID
    pub table_id: u64,
    /// 데이터베이스명
    pub database: String,
    /// 테이블명
    pub table: String,
    /// 컬럼 타입들
    pub column_types: Vec<u8>,
    /// 컬럼 메타데이터
    pub column_meta: Vec<Vec<u8>>,
    /// nullable 비트맵
    pub nullable_bitmap: Vec<u8>,
}

/// WRITE_ROWS 이벤트 데이터
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteRowsData {
    /// 테이블 ID
    pub table_id: u64,
    /// 플래그
    pub flags: u16,
    /// 컬럼 개수
    pub column_count: u64,
    /// 사용된 컬럼 비트맵
    pub columns_present: Vec<u8>,
    /// 행 데이터들
    pub rows: Vec<Vec<CellValue>>,
}

/// UPDATE_ROWS 이벤트 데이터
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateRowsData {
    /// 테이블 ID
    pub table_id: u64,
    /// 플래그
    pub flags: u16,
    /// 컬럼 개수
    pub column_count: u64,
    /// 사용된 컬럼 비트맵
    pub columns_present: Vec<u8>,
    /// 변경된 컬럼 비트맵
    pub columns_changed: Vec<u8>,
    /// 변경 전후 데이터 쌍들
    pub rows: Vec<(Vec<CellValue>, Vec<CellValue>)>,
}

/// DELETE_ROWS 이벤트 데이터
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteRowsData {
    /// 테이블 ID
    pub table_id: u64,
    /// 플래그
    pub flags: u16,
    /// 컬럼 개수
    pub column_count: u64,
    /// 사용된 컬럼 비트맵
    pub columns_present: Vec<u8>,
    /// 행 데이터들
    pub rows: Vec<Vec<CellValue>>,
}

/// 셀 값 (다양한 MySQL 타입 지원)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CellValue {
    Null,
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Float(f32),
    Double(f64),
    String(String),
    Bytes(Vec<u8>),
    DateTime(DateTime<Utc>),
    Date(String),
    Time(String),
    Decimal(String),
    Json(serde_json::Value),
}

impl CellValue {
    pub fn as_string(&self) -> Option<String> {
        match self {
            CellValue::String(s) => Some(s.clone()),
            CellValue::Int64(i) => Some(i.to_string()),
            CellValue::UInt64(u) => Some(u.to_string()),
            CellValue::Double(d) => Some(d.to_string()),
            CellValue::DateTime(dt) => Some(dt.to_rfc3339()),
            CellValue::Null => Some("NULL".to_string()),
            _ => None,
        }
    }
}

/// GTID 이벤트 데이터
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GtidEventData {
    /// GTID 문자열 (format: uuid:sequence-number)
    pub gtid: String,
    /// 커밋 플래그
    pub committed: bool,
}

/// 쿼리 이벤트 데이터
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryEventData {
    /// 스레드 ID
    pub thread_id: u32,
    /// 실행 시간 (초)
    pub exec_time: u32,
    /// 데이터베이스명
    pub database: String,
    /// 쿼리 문자열
    pub query: String,
}

/// 회전 이벤트 데이터
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RotateEventData {
    /// 새 바이너리 로그 파일명
    pub next_binlog_name: String,
    /// 새 파일의 시작 위치
    pub position: u64,
}

/// 모든 Binlog 이벤트를 포함하는 열거형
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BinlogEventData {
    TableMap(TableMapData),
    WriteRows(WriteRowsData),
    UpdateRows(UpdateRowsData),
    DeleteRows(DeleteRowsData),
    Query(QueryEventData),
    Rotate(RotateEventData),
    Gtid(GtidEventData),
    RowsQuery(String),
    Unknown(Vec<u8>),
}

/// 완성된 Binlog 이벤트
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinlogEvent {
    /// 이벤트 헤더
    pub header: EventHeader,
    /// 이벤트 데이터
    pub data: BinlogEventData,
}

/// CDC 변경 이벤트 (application-level view)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeEvent {
    /// GTID (있는 경우)
    pub gtid: Option<String>,
    /// 연산 타입 (INSERT, UPDATE, DELETE, DDL)
    pub op: OperationType,
    /// 타임스탬프
    pub timestamp: DateTime<Utc>,
    /// 데이터베이스명
    pub database: String,
    /// 테이블명
    pub table: String,
    /// 변경 전 데이터 (UPDATE/DELETE의 경우)
    pub before: Option<HashMap<String, CellValue>>,
    /// 변경 후 데이터 (INSERT/UPDATE의 경우)
    pub after: Option<HashMap<String, CellValue>>,
    /// 원본 쿼리 (DDL의 경우)
    pub query: Option<String>,
}

/// 변경 연산 타입
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OperationType {
    Insert,
    Update,
    Delete,
    Ddl,
}

impl OperationType {
    pub fn as_str(&self) -> &'static str {
        match self {
            OperationType::Insert => "INSERT",
            OperationType::Update => "UPDATE",
            OperationType::Delete => "DELETE",
            OperationType::Ddl => "DDL",
        }
    }
}
