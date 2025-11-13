# MySQL CDC (Change Data Capture) - Rust 구현

- MySQL 바이너리 로그(binlog)를 파싱하여 데이터 변경 사항을 추적합니다.

## 개요

### MySQL Binlog 기반

- MySQL은 일반 쿼리만으로는 CDC 구현이 불가능
- **MySQL Binlog**라는 전용 이벤트 로그를 사용하여 CDC 구현
- 바이너리 프로토콜로 인코딩된 이벤트 스트림

## 주요 구성 요소

### 1. **Binlog 파서** (`binlog.rs`)

- MySQL 바이너리 로그 파일 형식 파싱
- 이벤트 헤더 및 데이터 추출
- 지원 이벤트: TABLE_MAP, WRITE_ROWS, UPDATE_ROWS, DELETE_ROWS, QUERY, ROTATE, GTID

### 2. **GTID 관리** (`gtid.rs`)

- Global Transaction ID 추적
- GTID 집합 파싱 및 관리
- 형식: `uuid:sequence-number`
- 예: `550e8400-e29b-41d4-a716-446655440000:1-100,200,300-400`

### 3. **연결 관리** (`connection.rs`)

- MySQL 데이터베이스 연결
- Binlog 상태 조회
- 테이블 스키마 정보 수집
- GTID 모드 확인

### 4. **오프셋 추적** (`offset.rs`)

- Binlog 파일명 + 위치로 현재 처리 지점 저장
- `mysql-bin.000001:4096` 형식
- 재시작 시 정확한 지점부터 재개 가능

### 5. **이벤트 정의** (`events.rs`)

- Binlog 이벤트 타입 정의
- 셀 값(CellValue) 타입 정의
- 변경 이벤트(ChangeEvent) 구조

### 6. **CDC 엔진** (`cdc_engine.rs`)

- 전체 CDC 처리 오케스트레이션
- 스냅샷 처리 (초기 데이터 읽기)
- Binlog 스트리밍 (변경 사항 추적)
- 이벤트 변환 및 필터링

## 아키텍처

```
┌──────────────────────────┐
│   MySQL Database         │
└────────┬─────────────────┘
         │
         │ MySQL Replication Protocol
         │
    ┌────▼──────────────┐
    │  BinlogClient     │  ← TCP 스트림으로 이벤트 수신
    └────┬──────────────┘
         │
    ┌────▼──────────────┐
    │ BinlogParser      │  ← 바이너리 데이터 파싱
    └────┬──────────────┘
         │
    ┌────▼──────────────────────────────┐
    │ CdcEngine                          │  ← 변경 이벤트 생성
    │ ├─ Snapshot Processing            │
    │ ├─ Binlog Streaming              │
    │ ├─ Event Transformation          │
    │ └─ GTID Management               │
    └────┬──────────────────────────────┘
         │
    ┌────▼──────────────┐
    │ ChangeEvent       │  ← 최종 변경 이벤트
    │ Stream            │
    └───────────────────┘
```

## 사용법

### 기본 구조

```rust
use rust_mysql::cdc_engine::{CdcEngine, CdcConfig, SnapshotMode};
use rust_mysql::connection::ConnectionConfig;

#[tokio::main]
async fn main() -> Result<()> {
    // 1. 연결 설정
    let config = CdcConfig {
        connection: ConnectionConfig {
            hostname: "localhost".to_string(),
            port: 3306,
            username: "root".to_string(),
            password: "password".to_string(),
            database: Some("mydb".to_string()),
            server_id: 1,
            timeout: Duration::from_secs(30),
        },
        databases: vec!["mydb".to_string()],
        tables: None,
        snapshot_mode: SnapshotMode::Initial,
        include_ddl: true,
        gtid_filter: None,
    };

    // 2. CDC 엔진 생성
    let mut engine = CdcEngine::new(config);

    // 3. 시작
    engine.start().await?;

    // 4. 스냅샷 처리
    let snapshot_rx = engine.snapshot().await?;

    // 5. Binlog 스트리밍
    let binlog_rx = engine.stream_binlog().await?;

    // 6. 변경 이벤트 처리
    // snapshot_rx와 binlog_rx에서 ChangeEvent를 수신하여 처리

    Ok(())
}
```

### 환경 변수 설정

```bash
DB_HOST=localhost
DB_PORT=3306
DB_USER=root
DB_PASSWORD=password
DB_NAME=mydb
```

## Binlog 이벤트 타입

| 타입                  | 설명                       |
| --------------------- | -------------------------- |
| **GTID_EVENT**        | Global Transaction ID 지정 |
| **TABLE_MAP_EVENT**   | 테이블 스키마 정보         |
| **WRITE_ROWS_EVENT**  | INSERT 작업                |
| **UPDATE_ROWS_EVENT** | UPDATE 작업                |
| **DELETE_ROWS_EVENT** | DELETE 작업                |
| **QUERY_EVENT**       | DDL (CREATE, ALTER, DROP)  |
| **ROTATE_EVENT**      | Binlog 파일 로테이션       |

## 변경 이벤트 예시

```json
{
  "gtid": "550e8400-e29b-41d4-a716-446655440000:123",
  "op": "INSERT",
  "timestamp": "2024-11-13T10:30:00Z",
  "database": "mydb",
  "table": "users",
  "after": {
    "id": 1,
    "name": "John",
    "email": "john@example.com"
  }
}
```

## GTID 포맷

GTID(Global Transaction ID)는 MySQL 5.6+ 에서 복제를 추적하는 데 사용됩니다.

형식:

```
UUID:sequence-number
550e8400-e29b-41d4-a716-446655440000:1-100,200,300-400
```

- **UUID**: 서버 식별자
- **sequence-number**: 트랜잭션 일련 번호
- **범위**: `1-100` (1부터 100까지), `200` (단일 값)

## 스냅샷 모드

| 모드            | 설명                                |
| --------------- | ----------------------------------- |
| **Initial**     | 초기 스냅샷 + 이후 Binlog 스트리밍  |
| **SchemaOnly**  | 스키마만 읽기                       |
| **Never**       | 스냅샷 스킵 (Binlog부터 시작)       |
| **Incremental** | 증분 스냅샷 (재시작 후 놓친 데이터) |

## 테스트

### 단위 테스트

```bash
# 모든 테스트 실행
cargo test

# 라이브러리 테스트만
cargo test --lib

# 특정 테스트
cargo test test_gtid_parse
```

### Docker Compose로 MySQL 테스트

MySQL을 Docker로 실행하고 CDC 엔진을 테스트합니다.

#### 자동 설정 (권장)

```bash
# 1. 테스트 환경 자동 설정
./test.sh

# 2. CDC 엔진 실행
DB_HOST=localhost \
DB_PORT=3306 \
DB_USER=testuser \
DB_PASSWORD=testpass \
DB_NAME=testdb \
cargo run --release
```

#### 수동 설정

```bash
# 1. MySQL 컨테이너 시작
docker-compose up -d

# 2. MySQL 준비 대기 (약 10초)
sleep 10

# 3. CDC 엔진 실행
DB_HOST=localhost \
DB_PORT=3306 \
DB_USER=testuser \
DB_PASSWORD=testpass \
DB_NAME=testdb \
cargo run --release

# 4. 컨테이너 정지
docker-compose down
```

### 테스트 데이터

초기화 스크립트(`init.sql`)에서 다음 테이블이 생성됩니다:

- **users**: 사용자 정보
- **orders**: 주문 정보
- **order_items**: 주문 상품 상세

### 관리 패널

Admin 패널에서 데이터베이스 상태를 확인할 수 있습니다:

- **URL**: http://localhost:8080
- **Server**: mysql
- **User**: root
- **Password**: rootpassword
- **Database**: testdb

### 데이터 변경 테스트

MySQL 컨테이너에 접속하여 데이터를 수정하면 CDC 엔진이 변경사항을 감지합니다:

```bash
# MySQL 컨테이너 접속
docker-compose exec mysql mysql -u root -prootpassword testdb

# 데이터 삽입
INSERT INTO users (username, email) VALUES ('david', 'david@example.com');

# 데이터 수정
UPDATE orders SET status = 'cancelled' WHERE id = 1;

# 데이터 삭제
DELETE FROM order_items WHERE id = 1;
```

## 구현 현황

### 완료된 기능

- ✅ Binlog 파일 형식 파싱
- ✅ GTID 관리 및 파싱
- ✅ Binlog 이벤트 타입 정의
- ✅ 연결 관리 (JDBC 기반)
- ✅ 오프셋 추적
- ✅ CDC 엔진 구조
- ✅ 이벤트 변환 로직
- ✅ 단위 테스트

### 향후 개선사항

- [ ] MySQL Replication Protocol 구현 (직접 스트리밍)
- [ ] Kafka 연결 추가
- [ ] 스냅샷 병렬 처리
- [ ] 메트릭 수집
- [ ] 재시작 복구 로직
- [ ] 필터링 엔진
- [ ] DDL 분석 및 스키마 추적

## 성능 특성

### 메모리 사용

- GTID 집합: O(n) where n = GTID 범위 수
- 테이블 메타데이터: O(m) where m = 테이블 개수
- 변경 이벤트 버퍼: 설정 가능

### 처리량

- Binlog 파싱: 초당 수천 이벤트
- 메타데이터 캐싱으로 스키마 조회 최소화

## 참고 자료

- [MySQL Binlog Format](https://dev.mysql.com/doc/internals/en/binary-log.html)
- [GTID 개념](https://dev.mysql.com/doc/refman/8.0/en/replication-gtids.html)
- [Debezium MySQL Connector](https://debezium.io/documentation/reference/stable/connectors/mysql.html)

## 라이선스

MIT
