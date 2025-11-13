//! MySQL Binlog 프로토콜 파싱 및 Binlog 클라이언트
//!
//! Binlog 파일의 바이너리 형식을 파싱합니다.
//! 헤더: 4 바이트 매직 넘버 (0xfe 0x62 0x69 0x6e)
//! 각 이벤트:
//!   - Timestamp (4 bytes)
//!   - Type (1 byte)
//!   - Server ID (4 bytes)
//!   - Event Length (4 bytes)
//!   - Next Position (4 bytes)
//!   - Flags (2 bytes)
//!   - Event Data (variable)

use crate::error::{CdcError, Result};
use crate::events::*;
use crate::offset::SourceInfo;
use bytes::Buf;
use byteorder::{LittleEndian, ReadBytesExt};
use std::io::{Cursor, Read};
use std::sync::Arc;
use parking_lot::RwLock;
use tokio::sync::mpsc;

const BINLOG_MAGIC: &[u8] = &[0xfe, 0x62, 0x69, 0x6e]; // ".bin" in ASCII
const EVENT_HEADER_SIZE: usize = 19;

/// Binlog 파일 파서
pub struct BinlogParser;

impl BinlogParser {
    /// Binlog 파일 헤더 검증
    pub fn verify_magic(data: &[u8]) -> Result<()> {
        if data.len() < 4 {
            return Err(CdcError::BinlogParseError(
                "Invalid binlog: too short".to_string(),
            ));
        }

        if data[0..4] == BINLOG_MAGIC[..] {
            Ok(())
        } else {
            Err(CdcError::BinlogParseError(
                "Invalid binlog magic number".to_string(),
            ))
        }
    }

    /// 이벤트 헤더 파싱
    pub fn parse_header(data: &[u8]) -> Result<(EventHeader, usize)> {
        if data.len() < EVENT_HEADER_SIZE {
            return Err(CdcError::BinlogParseError(
                "Invalid event header: too short".to_string(),
            ));
        }

        let mut cursor = Cursor::new(data);

        let timestamp = cursor.read_u32::<LittleEndian>()?;
        let event_type = cursor.read_u8()?;
        let server_id = cursor.read_u32::<LittleEndian>()?;
        let event_length = cursor.read_u32::<LittleEndian>()?;
        let next_pos = cursor.read_u32::<LittleEndian>()?;
        let flags = cursor.read_u16::<LittleEndian>()?;

        Ok((
            EventHeader {
                timestamp,
                event_type: EventType::from_u8(event_type),
                server_id,
                event_length,
                next_pos,
                flags,
            },
            cursor.position() as usize,
        ))
    }

    /// 테이블 맵 이벤트 파싱 (19)
    pub fn parse_table_map_event(data: &[u8]) -> Result<TableMapData> {
        if data.len() < 8 {
            return Err(CdcError::BinlogParseError(
                "Invalid table map event".to_string(),
            ));
        }

        let mut cursor = Cursor::new(data);

        let table_id = cursor.read_u48::<LittleEndian>()
            .unwrap_or(0) as u64;
        let _flags = cursor.read_u16::<LittleEndian>()?;

        // 데이터베이스명 길이
        let db_len = cursor.read_u8()? as usize;
        let mut db_bytes = vec![0u8; db_len];
        cursor.read_exact(&mut db_bytes)?;
        let database = String::from_utf8_lossy(&db_bytes).to_string();

        // 테이블명 길이
        let tbl_len = cursor.read_u8()? as usize;
        let mut tbl_bytes = vec![0u8; tbl_len];
        cursor.read_exact(&mut tbl_bytes)?;
        let table = String::from_utf8_lossy(&tbl_bytes).to_string();

        // 컬럼 개수
        let column_count = read_lcb(&mut cursor)? as usize;
        let mut column_types = vec![0u8; column_count];
        cursor.read_exact(&mut column_types)?;

        // Metadata
        let metadata_length = read_lcb(&mut cursor)? as usize;
        let mut column_meta = vec![Vec::new(); column_count];

        let mut metadata_cursor = Cursor::new(vec![0u8; metadata_length]);
        cursor.read_exact(metadata_cursor.get_mut())?;

        // nullable bitmap
        let nullable_count = (column_count + 7) / 8;
        let mut nullable_bitmap = vec![0u8; nullable_count];
        cursor.read_exact(&mut nullable_bitmap)?;

        Ok(TableMapData {
            table_id,
            database,
            table,
            column_types,
            column_meta,
            nullable_bitmap,
        })
    }

    /// WRITE_ROWS 이벤트 파싱 (30)
    pub fn parse_write_rows_event(data: &[u8]) -> Result<WriteRowsData> {
        if data.len() < 6 {
            return Err(CdcError::BinlogParseError(
                "Invalid write rows event".to_string(),
            ));
        }

        let mut cursor = Cursor::new(data);

        let table_id = cursor.read_u48::<LittleEndian>()
            .unwrap_or(0) as u64;
        let flags = cursor.read_u16::<LittleEndian>()?;

        // 컬럼 개수
        let column_count = read_lcb(&mut cursor)?;

        // 컬럼 존재 비트맵
        let bitmap_bytes = (column_count as usize + 7) / 8;
        let mut columns_present = vec![0u8; bitmap_bytes];
        cursor.read_exact(&mut columns_present)?;

        // 행 데이터
        let rows = parse_row_data(&mut cursor, column_count as usize, &columns_present)?;

        Ok(WriteRowsData {
            table_id,
            flags,
            column_count,
            columns_present,
            rows,
        })
    }

    /// UPDATE_ROWS 이벤트 파싱 (31)
    pub fn parse_update_rows_event(data: &[u8]) -> Result<UpdateRowsData> {
        if data.len() < 6 {
            return Err(CdcError::BinlogParseError(
                "Invalid update rows event".to_string(),
            ));
        }

        let mut cursor = Cursor::new(data);

        let table_id = cursor.read_u48::<LittleEndian>()
            .unwrap_or(0) as u64;
        let flags = cursor.read_u16::<LittleEndian>()?;

        // 컬럼 개수
        let column_count = read_lcb(&mut cursor)?;

        // 컬럼 존재 비트맵
        let bitmap_bytes = (column_count as usize + 7) / 8;
        let mut columns_present = vec![0u8; bitmap_bytes];
        cursor.read_exact(&mut columns_present)?;

        // 변경된 컬럼 비트맵
        let mut columns_changed = vec![0u8; bitmap_bytes];
        cursor.read_exact(&mut columns_changed)?;

        // 변경 전후 데이터
        let mut rows = Vec::new();
        while (cursor.position() as usize) < data.len() {
            let before = parse_row_data(&mut cursor, column_count as usize, &columns_present)?;
            if before.is_empty() {
                break;
            }
            let after = parse_row_data(&mut cursor, column_count as usize, &columns_changed)?;
            if !after.is_empty() {
                rows.push((before[0].clone(), after[0].clone()));
            }
        }

        Ok(UpdateRowsData {
            table_id,
            flags,
            column_count,
            columns_present,
            columns_changed,
            rows,
        })
    }

    /// DELETE_ROWS 이벤트 파싱 (32)
    pub fn parse_delete_rows_event(data: &[u8]) -> Result<DeleteRowsData> {
        if data.len() < 6 {
            return Err(CdcError::BinlogParseError(
                "Invalid delete rows event".to_string(),
            ));
        }

        let mut cursor = Cursor::new(data);

        let table_id = cursor.read_u48::<LittleEndian>()
            .unwrap_or(0) as u64;
        let flags = cursor.read_u16::<LittleEndian>()?;

        // 컬럼 개수
        let column_count = read_lcb(&mut cursor)?;

        // 컬럼 존재 비트맵
        let bitmap_bytes = (column_count as usize + 7) / 8;
        let mut columns_present = vec![0u8; bitmap_bytes];
        cursor.read_exact(&mut columns_present)?;

        // 행 데이터
        let rows = parse_row_data(&mut cursor, column_count as usize, &columns_present)?;

        Ok(DeleteRowsData {
            table_id,
            flags,
            column_count,
            columns_present,
            rows,
        })
    }

    /// QUERY 이벤트 파싱 (2)
    pub fn parse_query_event(data: &[u8]) -> Result<QueryEventData> {
        if data.len() < 13 {
            return Err(CdcError::BinlogParseError(
                "Invalid query event".to_string(),
            ));
        }

        let mut cursor = Cursor::new(data);

        let thread_id = cursor.read_u32::<LittleEndian>()?;
        let exec_time = cursor.read_u32::<LittleEndian>()?;
        let db_len = cursor.read_u8()? as usize;
        let error_code = cursor.read_u16::<LittleEndian>()?;
        let status_len = cursor.read_u16::<LittleEndian>()? as usize;

        // Status variables skip
        cursor.set_position(cursor.position() + status_len as u64);

        // 데이터베이스명
        let mut db_bytes = vec![0u8; db_len];
        if db_len > 0 {
            cursor.read_exact(&mut db_bytes)?;
        }
        let database = String::from_utf8_lossy(&db_bytes).to_string();

        // null terminator skip
        cursor.read_u8().ok();

        // 쿼리
        let remaining = &data[cursor.position() as usize..];
        let query = String::from_utf8_lossy(remaining).to_string();

        Ok(QueryEventData {
            thread_id,
            exec_time,
            database,
            query,
        })
    }

    /// ROTATE 이벤트 파싱 (4)
    pub fn parse_rotate_event(data: &[u8]) -> Result<RotateEventData> {
        if data.len() < 8 {
            return Err(CdcError::BinlogParseError(
                "Invalid rotate event".to_string(),
            ));
        }

        let mut cursor = Cursor::new(data);

        let position = cursor.read_u64::<LittleEndian>()?;
        let filename_bytes = &data[cursor.position() as usize..];
        let filename = String::from_utf8_lossy(filename_bytes).to_string();

        Ok(RotateEventData {
            next_binlog_name: filename,
            position,
        })
    }

    /// GTID 이벤트 파싱 (33)
    pub fn parse_gtid_event(data: &[u8]) -> Result<GtidEventData> {
        if data.len() < 42 {
            return Err(CdcError::BinlogParseError(
                "Invalid GTID event".to_string(),
            ));
        }

        let mut cursor = Cursor::new(data);

        let _flags = cursor.read_u8()?;
        let mut uuid_bytes = [0u8; 16];
        cursor.read_exact(&mut uuid_bytes)?;

        let uuid = format_uuid(&uuid_bytes);

        let sequence = cursor.read_u64::<LittleEndian>()?;

        let gtid = format!("{}:{}", uuid, sequence);
        let committed = data[0] == 0;

        Ok(GtidEventData { gtid, committed })
    }
}

/// LCB (Length-Coded Binary) 읽기
fn read_lcb(cursor: &mut Cursor<&[u8]>) -> Result<u64> {
    let byte = cursor.read_u8()?;
    match byte {
        0..=0xfa => Ok(byte as u64),
        0xfb => Ok(0),
        0xfc => Ok(cursor.read_u24::<LittleEndian>()? as u64),
        0xfd => Ok(cursor.read_u24::<LittleEndian>()? as u64),
        0xfe => Ok(cursor.read_u64::<LittleEndian>()?),
        0xff => Err(CdcError::BinlogParseError("Invalid LCB value".to_string())),
    }
}

/// UUID 바이트 배열을 문자열로 변환
fn format_uuid(bytes: &[u8; 16]) -> String {
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0], bytes[1], bytes[2], bytes[3],
        bytes[4], bytes[5],
        bytes[6], bytes[7],
        bytes[8], bytes[9],
        bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]
    )
}

/// 행 데이터 파싱
fn parse_row_data(
    cursor: &mut Cursor<&[u8]>,
    column_count: usize,
    present_bitmap: &[u8],
) -> Result<Vec<Vec<CellValue>>> {
    let mut rows = Vec::new();
    let mut row = Vec::new();

    for col_idx in 0..column_count {
        let byte_idx = col_idx / 8;
        let bit_idx = col_idx % 8;

        if byte_idx >= present_bitmap.len() {
            row.push(CellValue::Null);
            continue;
        }

        let is_present = (present_bitmap[byte_idx] & (1 << bit_idx)) != 0;

        if !is_present {
            row.push(CellValue::Null);
        } else {
            // 컬럼 타입에 따라 파싱 (간단한 구현)
            if let Ok(byte) = cursor.read_u8() {
                match byte {
                    0 => row.push(CellValue::Null),
                    1 => {
                        if let Ok(val) = cursor.read_u8() {
                            row.push(CellValue::UInt8(val));
                        }
                    }
                    2 => {
                        if let Ok(val) = cursor.read_u16::<LittleEndian>() {
                            row.push(CellValue::UInt16(val));
                        }
                    }
                    4 => {
                        if let Ok(val) = cursor.read_u32::<LittleEndian>() {
                            row.push(CellValue::UInt32(val));
                        }
                    }
                    _ => row.push(CellValue::Bytes(vec![byte])),
                }
            }
        }
    }

    if !row.is_empty() {
        rows.push(row);
    }

    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_verify_magic() {
        let valid = vec![0xfe, 0x62, 0x69, 0x6e];
        assert!(BinlogParser::verify_magic(&valid).is_ok());

        let invalid = vec![0x00, 0x00, 0x00, 0x00];
        assert!(BinlogParser::verify_magic(&invalid).is_err());
    }

    #[test]
    fn test_format_uuid() {
        let bytes = [0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0,
                     0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0];
        let uuid = format_uuid(&bytes);
        assert!(uuid.contains('-'));
        assert_eq!(uuid.len(), 36);
    }

    #[test]
    fn test_binlog_client() {
        let client = BinlogClient::new(1, "mysql-bin.000001".to_string());
        let info = client.get_source_info();
        assert_eq!(info.server_id, 1);
        assert_eq!(info.binlog_filename, "mysql-bin.000001");
    }
}

/// Binlog 클라이언트 - Binlog 이벤트 스트림 처리
pub struct BinlogClient {
    source_info: Arc<RwLock<SourceInfo>>,
}

impl BinlogClient {
    pub fn new(server_id: u32, binlog_filename: String) -> Self {
        BinlogClient {
            source_info: Arc::new(RwLock::new(SourceInfo::new(server_id, binlog_filename))),
        }
    }

    /// Binlog 이벤트 스트림 시작
    pub async fn stream_events(&self) -> Result<mpsc::UnboundedReceiver<BinlogEvent>> {
        let (tx, rx) = mpsc::unbounded_channel();

        // 실제 구현에서는 MySQL replication protocol로 이벤트 수신
        // 여기서는 구조만 제시

        Ok(rx)
    }

    /// 현재 SourceInfo 반환
    pub fn get_source_info(&self) -> SourceInfo {
        self.source_info.read().clone()
    }

    /// SourceInfo 업데이트
    pub fn update_source_info<F>(&self, f: F)
    where
        F: FnOnce(&mut SourceInfo),
    {
        let mut info = self.source_info.write();
        f(&mut info);
    }
}
