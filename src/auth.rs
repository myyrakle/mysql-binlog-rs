//! MySQL 인증 처리
//!
//! Native password authentication 구현

use byteorder::{LittleEndian, WriteBytesExt};
use std::io::Write;

/// Client capability flags
pub mod capabilities {
    pub const LONG_PASSWORD: u32 = 1;
    pub const FOUND_ROWS: u32 = 2;
    pub const LONG_FLAG: u32 = 4;
    pub const CONNECT_WITH_DB: u32 = 8;
    pub const NO_SCHEMA: u32 = 16;
    pub const COMPRESS: u32 = 32;
    pub const ODBC: u32 = 64;
    pub const LOCAL_FILES: u32 = 128;
    pub const IGNORE_SPACE: u32 = 256;
    pub const PROTOCOL_41: u32 = 512;
    pub const INTERACTIVE: u32 = 1024;
    pub const SSL: u32 = 2048;
    pub const IGNORE_SIGPIPE: u32 = 4096;
    pub const TRANSACTIONS: u32 = 8192;
    pub const SECURE_CONNECTION: u32 = 32768;
    pub const MULTI_STATEMENTS: u32 = 1 << 16;
    pub const MULTI_RESULTS: u32 = 1 << 17;
    pub const PS_MULTI_RESULTS: u32 = 1 << 18;
    pub const PLUGIN_AUTH: u32 = 1 << 19;
    pub const CONNECT_ATTRS: u32 = 1 << 20;
}

/// Native password 인증 응답 생성
pub fn create_auth_response(password: &str, scramble: &[u8]) -> Vec<u8> {
    if password.is_empty() {
        return Vec::new();
    }

    // SHA1(password)
    let stage1 = sha1(password.as_bytes());

    // SHA1(SHA1(password))
    let stage2 = sha1(&stage1);

    // SHA1(scramble + SHA1(SHA1(password)))
    let mut combined = scramble.to_vec();
    combined.extend_from_slice(&stage2);
    let stage3 = sha1(&combined);

    // XOR(SHA1(password), SHA1(scramble + SHA1(SHA1(password))))
    let mut result = Vec::with_capacity(20);
    for i in 0..20 {
        result.push(stage1[i] ^ stage3[i]);
    }

    result
}

/// SHA1 해시 계산 (SHA1을 사용하는 것이 MySQL native password의 표준)
fn sha1(data: &[u8]) -> Vec<u8> {
    use sha1::{Digest, Sha1};
    let mut hasher = Sha1::new();
    hasher.update(data);
    hasher.finalize().to_vec()
}

/// 인증 패킷 생성
pub fn create_handshake_response(
    username: &str,
    password: &str,
    database: Option<&str>,
    scramble: &[u8],
    collation: u8,
) -> Result<Vec<u8>, std::io::Error> {
    let mut buffer = Vec::new();

    // Client capability flags (4 bytes)
    let mut capabilities = capabilities::LONG_PASSWORD
        | capabilities::LONG_FLAG
        | capabilities::PROTOCOL_41
        | capabilities::SECURE_CONNECTION
        | capabilities::MULTI_STATEMENTS
        | capabilities::MULTI_RESULTS
        | capabilities::PLUGIN_AUTH;

    if database.is_some() {
        capabilities |= capabilities::CONNECT_WITH_DB;
    }

    buffer.write_u32::<LittleEndian>(capabilities)?;

    // Max packet size (4 bytes) - 0 means default (16MB)
    buffer.write_u32::<LittleEndian>(0)?;

    // Character set (1 byte)
    buffer.write_u8(collation)?;

    // Reserved (23 bytes of zeros)
    buffer.write_all(&[0u8; 23])?;

    // Username (null-terminated string)
    buffer.write_all(username.as_bytes())?;
    buffer.write_u8(0)?;

    // Authentication response
    let auth_response = create_auth_response(password, scramble);
    buffer.write_u8(auth_response.len() as u8)?;
    buffer.write_all(&auth_response)?;

    // Database name (null-terminated string, if provided)
    if let Some(db) = database {
        buffer.write_all(db.as_bytes())?;
        buffer.write_u8(0)?;
    }

    // Authentication plugin name (null-terminated)
    buffer.write_all(b"mysql_native_password")?;
    buffer.write_u8(0)?;

    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_auth_response_empty_password() {
        let response = create_auth_response("", &[1, 2, 3, 4]);
        assert!(response.is_empty());
    }

    #[test]
    fn test_create_auth_response() {
        let scramble = vec![0x40, 0x3B, 0x57, 0x68, 0x3A, 0x77, 0x23, 0x29];
        let response = create_auth_response("password", &scramble);
        assert_eq!(response.len(), 20); // SHA1 produces 20 bytes
    }

    #[test]
    fn test_create_handshake_response() {
        let scramble = vec![0x40, 0x3B, 0x57, 0x68, 0x3A, 0x77, 0x23, 0x29];
        let packet = create_handshake_response(
            "root",
            "password",
            Some("testdb"),
            &scramble,
            33, // utf8_general_ci
        )
        .unwrap();

        // 패킷이 합리적인 크기인지 확인
        assert!(packet.len() > 50);
    }
}
