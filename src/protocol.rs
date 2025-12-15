//! MySQL 프로토콜 패킷 처리
//!
//! mysql-binlog-connector-java의 PacketChannel과 동일한 기능 제공

use crate::error::{CdcError, Result};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::Read;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::debug;

/// MySQL 패킷 채널
pub struct PacketChannel {
    stream: TcpStream,
}

impl PacketChannel {
    /// 새 패킷 채널 생성 (TCP 연결)
    pub async fn connect(hostname: &str, port: u16) -> Result<Self> {
        let addr = format!("{}:{}", hostname, port);
        let stream = TcpStream::connect(&addr)
            .await
            .map_err(|e| CdcError::ConnectionError(format!("Failed to connect to {}: {}", addr, e)))?;

        debug!("Connected to MySQL at {}", addr);

        Ok(PacketChannel { stream })
    }

    /// 패킷 읽기
    pub async fn read_packet(&mut self) -> Result<Vec<u8>> {
        // 패킷 헤더 읽기 (3 bytes length + 1 byte sequence)
        let mut len_buf = [0u8; 3];
        self.stream.read_exact(&mut len_buf).await
            .map_err(|e| CdcError::IoError(format!("Failed to read packet length: {}", e)))?;
        let length = u32::from_le_bytes([len_buf[0], len_buf[1], len_buf[2], 0]);

        let _sequence = self.stream.read_u8().await
            .map_err(|e| CdcError::IoError(format!("Failed to read sequence: {}", e)))?;

        // 패킷 본문 읽기
        let mut buffer = vec![0u8; length as usize];
        self.stream.read_exact(&mut buffer).await
            .map_err(|e| CdcError::IoError(format!("Failed to read packet body: {}", e)))?;

        Ok(buffer)
    }

    /// 패킷 쓰기
    pub async fn write_packet(&mut self, data: &[u8], sequence: u8) -> Result<()> {
        let length = data.len() as u32;

        // 패킷 헤더 작성
        let mut header = Vec::new();
        WriteBytesExt::write_u24::<LittleEndian>(&mut header, length)
            .map_err(|e| CdcError::IoError(format!("Failed to write length: {}", e)))?;
        WriteBytesExt::write_u8(&mut header, sequence)
            .map_err(|e| CdcError::IoError(format!("Failed to write sequence: {}", e)))?;

        // 전송
        self.stream.write_all(&header).await
            .map_err(|e| CdcError::IoError(format!("Failed to write header: {}", e)))?;
        self.stream.write_all(data).await
            .map_err(|e| CdcError::IoError(format!("Failed to write data: {}", e)))?;
        self.stream.flush().await
            .map_err(|e| CdcError::IoError(format!("Failed to flush: {}", e)))?;

        Ok(())
    }

    /// 스트림에서 직접 읽기 (binlog 이벤트용)
    pub async fn read_raw(&mut self, buffer: &mut [u8]) -> Result<usize> {
        self.stream.read(buffer).await
            .map_err(|e| CdcError::IoError(format!("Failed to read raw data: {}", e)))
    }
}

/// Greeting 패킷 파싱
pub struct GreetingPacket {
    pub protocol_version: u8,
    pub server_version: String,
    pub thread_id: u32,
    pub scramble: Vec<u8>,
    pub server_capabilities: u32,
    pub server_collation: u8,
    pub server_status: u16,
}

impl GreetingPacket {
    pub fn parse(data: &[u8]) -> Result<Self> {
        let mut cursor = std::io::Cursor::new(data);

        // Protocol version (1 byte)
        let protocol_version = ReadBytesExt::read_u8(&mut cursor)
            .map_err(|e| CdcError::ProtocolError(format!("Failed to read protocol version: {}", e)))?;

        // Server version (null-terminated string)
        let server_version = read_null_terminated_string(&mut cursor)?;

        // Thread ID (4 bytes)
        let thread_id = ReadBytesExt::read_u32::<LittleEndian>(&mut cursor)
            .map_err(|e| CdcError::ProtocolError(format!("Failed to read thread ID: {}", e)))?;

        // Auth plugin data part 1 (8 bytes)
        let mut scramble_part1 = vec![0u8; 8];
        Read::read_exact(&mut cursor, &mut scramble_part1)
            .map_err(|e| CdcError::ProtocolError(format!("Failed to read scramble part 1: {}", e)))?;

        // Filler (1 byte, always 0x00)
        ReadBytesExt::read_u8(&mut cursor)
            .map_err(|e| CdcError::ProtocolError(format!("Failed to read filler: {}", e)))?;

        // Capability flags (lower 2 bytes)
        let capabilities_lower = ReadBytesExt::read_u16::<LittleEndian>(&mut cursor)
            .map_err(|e| CdcError::ProtocolError(format!("Failed to read capabilities: {}", e)))?;

        // Character set (1 byte)
        let server_collation = ReadBytesExt::read_u8(&mut cursor)
            .map_err(|e| CdcError::ProtocolError(format!("Failed to read collation: {}", e)))?;

        // Status flags (2 bytes)
        let server_status = ReadBytesExt::read_u16::<LittleEndian>(&mut cursor)
            .map_err(|e| CdcError::ProtocolError(format!("Failed to read status: {}", e)))?;

        // Capability flags (upper 2 bytes)
        let capabilities_upper = ReadBytesExt::read_u16::<LittleEndian>(&mut cursor)
            .map_err(|e| CdcError::ProtocolError(format!("Failed to read capabilities upper: {}", e)))?;

        let server_capabilities = (capabilities_upper as u32) << 16 | capabilities_lower as u32;

        // Length of auth plugin data (1 byte)
        let auth_data_len = ReadBytesExt::read_u8(&mut cursor)
            .map_err(|e| CdcError::ProtocolError(format!("Failed to read auth data length: {}", e)))?;

        // Reserved (10 bytes)
        let mut reserved = vec![0u8; 10];
        Read::read_exact(&mut cursor, &mut reserved)
            .map_err(|e| CdcError::ProtocolError(format!("Failed to read reserved: {}", e)))?;

        // Auth plugin data part 2 (at least 13 bytes)
        let scramble_len = std::cmp::max(13, auth_data_len.saturating_sub(8)) as usize;
        let mut scramble_part2 = vec![0u8; scramble_len];
        Read::read_exact(&mut cursor, &mut scramble_part2)
            .map_err(|e| CdcError::ProtocolError(format!("Failed to read scramble part 2: {}", e)))?;

        // Combine scramble parts
        let mut scramble = scramble_part1;
        scramble.extend_from_slice(&scramble_part2[..scramble_part2.len()-1]); // 마지막 null byte 제외

        Ok(GreetingPacket {
            protocol_version,
            server_version,
            thread_id,
            scramble,
            server_capabilities,
            server_collation,
            server_status,
        })
    }
}

/// null로 끝나는 문자열 읽기
fn read_null_terminated_string<R: Read>(reader: &mut R) -> Result<String> {
    let mut bytes = Vec::new();
    loop {
        let byte = ReadBytesExt::read_u8(reader)
            .map_err(|e| CdcError::ProtocolError(format!("Failed to read string byte: {}", e)))?;
        if byte == 0 {
            break;
        }
        bytes.push(byte);
    }
    String::from_utf8(bytes)
        .map_err(|e| CdcError::ProtocolError(format!("Invalid UTF-8 in string: {}", e)))
}

/// Error 패킷 확인
pub fn is_error_packet(data: &[u8]) -> bool {
    !data.is_empty() && data[0] == 0xFF
}

/// OK 패킷 확인
pub fn is_ok_packet(data: &[u8]) -> bool {
    !data.is_empty() && data[0] == 0x00
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_error_packet() {
        assert!(is_error_packet(&[0xFF, 0x01, 0x02]));
        assert!(!is_error_packet(&[0x00, 0x01, 0x02]));
    }

    #[test]
    fn test_is_ok_packet() {
        assert!(is_ok_packet(&[0x00, 0x01, 0x02]));
        assert!(!is_ok_packet(&[0xFF, 0x01, 0x02]));
    }
}
