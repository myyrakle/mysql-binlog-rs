//! MySQL Binlog CDC (Change Data Capture) 핵심 구현
//!
//! 이 라이브러리는 MySQL 바이너리 로그를 읽어 데이터 변경 사항을 추적합니다.
//! 주요 기능:
//! - Binlog 프로토콜 파싱
//! - GTID (Global Transaction ID) 관리
//! - 변경 이벤트 추적
//! - 연결 관리 및 재시작

pub mod binlog;
pub mod connection;
pub mod cdc_engine;
pub mod events;
pub mod error;
pub mod gtid;
pub mod offset;

pub use binlog::BinlogClient;
pub use cdc_engine::CdcEngine;
pub use connection::MySqlConnection;
pub use events::{BinlogEvent, EventType, ChangeEvent};
pub use error::{CdcError, Result};
pub use gtid::GtidSet;
pub use offset::SourceInfo;
