//! MySQL 데이터베이스 연결 관리

use crate::error::{CdcError, Result};
use crate::gtid::GtidSet;
use mysql_async::prelude::*;
use mysql_async::{Conn, Opts, OptsBuilder};
use std::time::Duration;

/// MySQL 연결 설정
#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    pub hostname: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub database: Option<String>,
    pub server_id: u32,
    pub timeout: Duration,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        ConnectionConfig {
            hostname: "localhost".to_string(),
            port: 3306,
            username: "root".to_string(),
            password: String::new(),
            database: None,
            server_id: 1,
            timeout: Duration::from_secs(30),
        }
    }
}

impl ConnectionConfig {
    pub fn new(hostname: impl Into<String>, username: impl Into<String>) -> Self {
        ConnectionConfig {
            hostname: hostname.into(),
            username: username.into(),
            ..Default::default()
        }
    }

    fn build_opts(&self) -> Result<Opts> {
        let connection_string = if let Some(ref db) = self.database {
            format!(
                "mysql://{}:{}@{}:{}/{}",
                self.username, self.password, self.hostname, self.port, db
            )
        } else {
            format!(
                "mysql://{}:{}@{}:{}",
                self.username, self.password, self.hostname, self.port
            )
        };

        connection_string
            .parse()
            .map_err(|_| CdcError::ConnectionError("Failed to parse connection string".to_string()))
    }
}

/// MySQL 연결 래퍼
pub struct MySqlConnection {
    conn: Conn,
    config: ConnectionConfig,
}

impl MySqlConnection {
    pub async fn connect(config: ConnectionConfig) -> Result<Self> {
        let opts = config.build_opts()?;
        let pool = mysql_async::Pool::new(opts);

        let conn = pool
            .get_conn()
            .await
            .map_err(|e| CdcError::ConnectionError(format!("Failed to connect to MySQL: {}", e)))?;

        Ok(MySqlConnection { conn, config })
    }

    /// Binlog 상태 조회
    pub async fn get_binlog_status(&mut self) -> Result<BinlogStatus> {
        let result: Vec<(String, u64, String, String, String)> = self.conn
            .query("SHOW BINARY LOG STATUS")
            .await
            .map_err(|e| CdcError::QueryError(format!("Failed to query binlog status: {}", e)))?;

        if result.is_empty() {
            return Err(CdcError::QueryError("No binlog status available".to_string()));
        }

        let (file, position, binlog_do_db, binlog_ignore_db, executed_gtid_set) = result[0].clone();

        Ok(BinlogStatus {
            file: file.clone(),
            position,
            binlog_do_db: if binlog_do_db.is_empty() { None } else { Some(binlog_do_db) },
            binlog_ignore_db: if binlog_ignore_db.is_empty() { None } else { Some(binlog_ignore_db) },
            executed_gtid_set: GtidSet::parse(&executed_gtid_set).unwrap_or_default(),
        })
    }

    /// GTID 모드 활성 여부 확인
    pub async fn is_gtid_mode_enabled(&mut self) -> Result<bool> {
        let result: Vec<(String, String)> = self.conn
            .query("SHOW GLOBAL VARIABLES LIKE 'GTID_MODE'")
            .await
            .map_err(|e| CdcError::QueryError(format!("Failed to query GTID mode: {}", e)))?;

        Ok(!result.is_empty() && result[0].1 == "ON")
    }

    /// 현재 실행된 GTID 집합 조회
    pub async fn get_executed_gtid_set(&mut self) -> Result<GtidSet> {
        let result: Vec<(String,)> = self.conn
            .query("SELECT @@global.gtid_executed")
            .await
            .map_err(|e| CdcError::QueryError(format!("Failed to query gtid_executed: {}", e)))?;

        if result.is_empty() {
            return Ok(GtidSet::new());
        }

        GtidSet::parse(&result[0].0)
    }

    /// MySQL 서버 정보 조회
    pub async fn get_server_id(&mut self) -> Result<u32> {
        let result: Vec<(u32,)> = self.conn
            .query("SELECT @@server_id")
            .await
            .map_err(|e| CdcError::QueryError(format!("Failed to query server_id: {}", e)))?;

        if result.is_empty() {
            return Ok(self.config.server_id);
        }

        Ok(result[0].0)
    }

    /// 변수 조회
    pub async fn get_variable(&mut self, name: &str) -> Result<Option<String>> {
        let query = format!("SHOW GLOBAL VARIABLES LIKE '{}'", name);
        let result: Vec<(String, String)> = self.conn
            .query(&query)
            .await
            .map_err(|e| CdcError::QueryError(format!("Failed to query {}: {}", name, e)))?;

        Ok(result.first().map(|(_, v)| v.clone()))
    }

    /// Binlog 형식 확인 (ROW, STATEMENT, MIXED)
    pub async fn get_binlog_format(&mut self) -> Result<String> {
        self.get_variable("binlog_format")
            .await?
            .ok_or_else(|| CdcError::QueryError("Binlog format not found".to_string()))
    }

    /// 테이블 스키마 조회
    pub async fn get_table_schema(&mut self, database: &str, table: &str) -> Result<Vec<ColumnInfo>> {
        let query = format!(
            "SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_KEY, EXTRA \
             FROM INFORMATION_SCHEMA.COLUMNS \
             WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}' \
             ORDER BY ORDINAL_POSITION",
            database, table
        );

        let result: Vec<(String, String, String, String, String)> = self.conn
            .query(&query)
            .await
            .map_err(|e| CdcError::QueryError(format!("Failed to query table schema: {}", e)))?;

        Ok(result
            .into_iter()
            .map(|(name, column_type, is_nullable, column_key, extra)| ColumnInfo {
                name,
                column_type,
                nullable: is_nullable == "YES",
                is_key: !column_key.is_empty(),
                extra,
            })
            .collect())
    }

    /// 모든 데이터베이스 나열
    pub async fn get_databases(&mut self) -> Result<Vec<String>> {
        let result: Vec<(String,)> = self.conn
            .query("SHOW DATABASES")
            .await
            .map_err(|e| CdcError::QueryError(format!("Failed to query databases: {}", e)))?;

        Ok(result.into_iter().map(|(db,)| db).collect())
    }

    /// 데이터베이스의 모든 테이블 나열
    pub async fn get_tables(&mut self, database: &str) -> Result<Vec<String>> {
        let query = format!("SHOW TABLES FROM `{}`", database);
        let result: Vec<(String,)> = self.conn
            .query(&query)
            .await
            .map_err(|e| CdcError::QueryError(format!("Failed to query tables: {}", e)))?;

        Ok(result.into_iter().map(|(table,)| table).collect())
    }

    pub async fn close(&mut self) -> Result<()> {
        // mysql_async::Conn는 Drop 시 자동으로 정리됨
        Ok(())
    }
}

/// Binlog 상태
#[derive(Debug, Clone)]
pub struct BinlogStatus {
    pub file: String,
    pub position: u64,
    pub binlog_do_db: Option<String>,
    pub binlog_ignore_db: Option<String>,
    pub executed_gtid_set: GtidSet,
}

/// 테이블 컬럼 정보
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub column_type: String,
    pub nullable: bool,
    pub is_key: bool,
    pub extra: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_config_default() {
        let config = ConnectionConfig::default();
        assert_eq!(config.hostname, "localhost");
        assert_eq!(config.port, 3306);
    }

    #[test]
    fn test_connection_config_new() {
        let config = ConnectionConfig::new("127.0.0.1", "root");
        assert_eq!(config.hostname, "127.0.0.1");
        assert_eq!(config.username, "root");
    }
}
