//! CDC 관련 에러 타입

use thiserror::Error;
use std::io;

#[derive(Error, Debug)]
pub enum CdcError {
    #[error("MySQL 연결 에러: {0}")]
    ConnectionError(String),

    #[error("Binlog 파싱 에러: {0}")]
    BinlogParseError(String),

    #[error("유효하지 않은 이벤트: {0}")]
    InvalidEvent(String),

    #[error("GTID 처리 에러: {0}")]
    GtidError(String),

    #[error("쿼리 실행 에러: {0}")]
    QueryError(String),

    #[error("I/O 에러: {0}")]
    IoError(#[from] io::Error),

    #[error("직렬화 에러: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("Timeout 에러")]
    Timeout,

    #[error("예상치 못한 에러: {0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, CdcError>;
