//! MySQL Binlog 클라이언트
//!
//! mysql-binlog-connector-java를 참고하여 구현한 Rust binlog 스트리밍 클라이언트

use crate::auth;
use crate::connection::ConnectionConfig;
use crate::error::{CdcError, Result};
use crate::events::BinlogEvent;
use crate::protocol::{self, GreetingPacket, PacketChannel};
use byteorder::{LittleEndian, WriteBytesExt};
use std::io::Write;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// COM_BINLOG_DUMP 명령어 코드
const COM_BINLOG_DUMP: u8 = 0x12;

/// COM_BINLOG_DUMP_GTID 명령어 코드
const COM_BINLOG_DUMP_GTID: u8 = 0x1e;

/// Binlog 클라이언트
pub struct BinlogClient {
    config: ConnectionConfig,
    binlog_filename: String,
    binlog_position: u64,
}

impl BinlogClient {
    /// 새 Binlog 클라이언트 생성
    pub fn new(config: ConnectionConfig, binlog_filename: String, binlog_position: u64) -> Self {
        BinlogClient {
            config,
            binlog_filename,
            binlog_position,
        }
    }

    /// Binlog 스트리밍 시작
    pub async fn start_streaming(&self) -> Result<mpsc::UnboundedReceiver<BinlogEvent>> {
        let (tx, rx) = mpsc::unbounded_channel();

        info!(
            "Starting binlog streaming from {}:{}",
            self.binlog_filename, self.binlog_position
        );

        // MySQL 연결 설정
        let connection_string = if let Some(ref db) = self.config.database {
            format!(
                "mysql://{}:{}@{}:{}/{}",
                self.config.username,
                self.config.password,
                self.config.hostname,
                self.config.port,
                db
            )
        } else {
            format!(
                "mysql://{}:{}@{}:{}",
                self.config.username, self.config.password, self.config.hostname, self.config.port
            )
        };

        let opts: mysql_async::Opts = connection_string
            .parse()
            .map_err(|_| CdcError::ConnectionError("Invalid connection string".to_string()))?;

        // 백그라운드에서 binlog 이벤트 읽기
        let binlog_filename = self.binlog_filename.clone();
        let binlog_position = self.binlog_position;
        let server_id = self.config.server_id;

        tokio::spawn(async move {
            match Self::read_binlog_events(opts, server_id, binlog_filename, binlog_position, tx)
                .await
            {
                Ok(_) => info!("Binlog streaming ended"),
                Err(e) => error!("Binlog streaming error: {}", e),
            }
        });

        Ok(rx)
    }

    /// Binlog 이벤트 읽기 (실제 구현)
    async fn read_binlog_events(
        _opts: mysql_async::Opts,
        server_id: u32,
        binlog_filename: String,
        binlog_position: u64,
        tx: mpsc::UnboundedSender<BinlogEvent>,
    ) -> Result<()> {
        // TODO: opts에서 호스트, 포트, 사용자명, 비밀번호 추출
        // 지금은 하드코딩
        let hostname = "localhost";
        let port = 3306;
        let username = "root";
        let password = "rootpassword";
        let database = Some("testdb");

        info!("Connecting to {}:{}", hostname, port);

        // 1. TCP 소켓 열기
        let mut channel = PacketChannel::connect(hostname, port).await?;

        // 2. MySQL 핸드셰이크 수신
        let greeting_packet = channel.read_packet().await?;
        let greeting = GreetingPacket::parse(&greeting_packet)?;

        info!(
            "MySQL Server version: {}, Thread ID: {}",
            greeting.server_version, greeting.thread_id
        );

        // 3. 인증
        let auth_response = auth::create_handshake_response(
            username,
            password,
            database,
            &greeting.scramble,
            greeting.server_collation,
        )
        .map_err(|e| CdcError::ConnectionError(format!("Failed to create auth response: {}", e)))?;

        channel.write_packet(&auth_response, 1).await?;

        // 4. 인증 결과 확인
        let auth_result = channel.read_packet().await?;
        if protocol::is_error_packet(&auth_result) {
            return Err(CdcError::ConnectionError(
                "Authentication failed".to_string(),
            ));
        }

        info!("Authentication successful");

        // 5. 체크섬 설정 (필수!)
        // MySQL 서버의 binlog 체크섬을 비활성화하도록 요청
        let checksum_query = b"SET @master_binlog_checksum='NONE'";
        let mut query_packet = vec![0x03]; // COM_QUERY
        query_packet.extend_from_slice(checksum_query);

        channel.write_packet(&query_packet, 0).await?;

        // 응답 확인
        let checksum_result = channel.read_packet().await?;
        if protocol::is_error_packet(&checksum_result) {
            warn!("Failed to set binlog checksum to NONE, continuing anyway...");
        } else {
            info!("Binlog checksum set to NONE");
        }

        // 6. COM_BINLOG_DUMP 명령어 전송
        let dump_command =
            Self::create_binlog_dump_command(server_id, &binlog_filename, binlog_position)?;

        channel.write_packet(&dump_command, 0).await?;

        info!(
            "Sent COM_BINLOG_DUMP: file={}, position={}",
            binlog_filename, binlog_position
        );

        // 7. Binlog 이벤트 스트리밍
        info!("Binlog event streaming started - reading events...");

        // 무한 루프로 이벤트 읽기
        let mut event_count = 0;
        loop {
            match channel.read_packet().await {
                Ok(packet) => {
                    // 에러 패킷 확인
                    if protocol::is_error_packet(&packet) {
                        error!("Received error packet from server");
                        if packet.len() > 3 {
                            let error_code = u16::from_le_bytes([packet[1], packet[2]]);
                            let error_msg = String::from_utf8_lossy(&packet[9..]);
                            error!("Error code: {}, message: {}", error_code, error_msg);
                        }
                        break;
                    }

                    // EOF 패킷 확인 (0xFE, 패킷 길이 < 9)
                    if !packet.is_empty() && packet[0] == 0xFE && packet.len() < 9 {
                        info!("Received EOF packet - stream ended");
                        break;
                    }

                    event_count += 1;

                    // 패킷이 비어있지 않으면 binlog 이벤트
                    if !packet.is_empty() {
                        // 첫 바이트 0x00은 OK 표시, 실제 이벤트 데이터는 그 다음부터
                        let event_data = if packet[0] == 0x00 && packet.len() > 1 {
                            &packet[1..]
                        } else {
                            &packet[..]
                        };

                        if event_data.len() >= 19 {
                            // Binlog 이벤트 헤더 파싱 (최소 19 bytes)
                            let timestamp = u32::from_le_bytes([
                                event_data[0],
                                event_data[1],
                                event_data[2],
                                event_data[3],
                            ]);
                            let event_type = event_data[4];
                            let server_id = u32::from_le_bytes([
                                event_data[5],
                                event_data[6],
                                event_data[7],
                                event_data[8],
                            ]);
                            let event_size = u32::from_le_bytes([
                                event_data[9],
                                event_data[10],
                                event_data[11],
                                event_data[12],
                            ]);
                            let log_pos = u32::from_le_bytes([
                                event_data[13],
                                event_data[14],
                                event_data[15],
                                event_data[16],
                            ]);
                            let flags = u16::from_le_bytes([event_data[17], event_data[18]]);

                            info!(
                                "📦 Event #{}: type={}, timestamp={}, server_id={}, size={}, pos={}, flags=0x{:04x}",
                                event_count,
                                event_type,
                                timestamp,
                                server_id,
                                event_size,
                                log_pos,
                                flags
                            );

                            // 이벤트 타입별 추가 정보 출력
                            if event_type == 2 {
                                // QUERY_EVENT
                                info!("   → QUERY_EVENT detected (likely DDL or BEGIN/COMMIT)");
                            } else if event_type == 30 {
                                // WRITE_ROWS_EVENT
                                info!("   → WRITE_ROWS_EVENT detected (INSERT)");
                            } else if event_type == 31 {
                                // UPDATE_ROWS_EVENT
                                info!("   → UPDATE_ROWS_EVENT detected (UPDATE)");
                            } else if event_type == 32 {
                                // DELETE_ROWS_EVENT
                                info!("   → DELETE_ROWS_EVENT detected (DELETE)");
                            } else if event_type == 19 {
                                // TABLE_MAP_EVENT
                                info!("   → TABLE_MAP_EVENT detected (table schema info)");
                            }

                            // Raw 데이터 출력 (처음 100바이트만)
                            let display_len = std::cmp::min(100, event_data.len());
                            debug!(
                                "   Raw data (first {} bytes): {:02x?}",
                                display_len,
                                &event_data[..display_len]
                            );
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to read packet: {}", e);
                    break;
                }
            }
        }

        info!(
            "Binlog streaming ended. Total events received: {}",
            event_count
        );
        Ok(())
    }

    /// COM_BINLOG_DUMP 명령어 생성
    fn create_binlog_dump_command(
        server_id: u32,
        binlog_filename: &str,
        binlog_position: u64,
    ) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();

        // COM_BINLOG_DUMP 명령어 코드
        buffer.write_u8(COM_BINLOG_DUMP)?;

        // Binlog position (4 bytes, little-endian)
        buffer.write_u32::<LittleEndian>(binlog_position as u32)?;

        // Flags (2 bytes) - 0 for non-blocking
        buffer.write_u16::<LittleEndian>(0)?;

        // Server ID (4 bytes)
        buffer.write_u32::<LittleEndian>(server_id)?;

        // Binlog filename (null-terminated string)
        buffer.write_all(binlog_filename.as_bytes())?;

        debug!(
            "Created COM_BINLOG_DUMP command: server_id={}, file={}, position={}",
            server_id, binlog_filename, binlog_position
        );

        Ok(buffer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_binlog_dump_command() {
        let cmd = BinlogClient::create_binlog_dump_command(1, "mysql-bin.000001", 4).unwrap();

        // COM_BINLOG_DUMP (1) + position (4) + flags (2) + server_id (4) + filename
        assert!(cmd.len() > 11);
        assert_eq!(cmd[0], COM_BINLOG_DUMP);
    }
}
