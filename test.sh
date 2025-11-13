#!/bin/bash

set -e

echo "====================================="
echo "MySQL CDC Rust Implementation Test"
echo "====================================="
echo ""

# Color codes
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 1. Docker Compose 시작
echo -e "${BLUE}[1/5] Starting Docker containers...${NC}"
docker-compose up -d
echo -e "${GREEN}✓ Docker containers started${NC}"
echo ""

# 2. MySQL 준비 대기
echo -e "${BLUE}[2/5] Waiting for MySQL to be ready...${NC}"
sleep 5
for i in {1..30}; do
    if docker exec rust_mysql_cdc_test mysqladmin ping -h localhost -u root -prootpassword &> /dev/null; then
        echo -e "${GREEN}✓ MySQL is ready${NC}"
        break
    fi
    echo "Waiting for MySQL... ($i/30)"
    sleep 2
done
echo ""

# 3. Binlog 상태 확인
echo -e "${BLUE}[3/5] Checking MySQL Binlog status...${NC}"
docker exec rust_mysql_cdc_test mysql -u root -prootpassword -e "SHOW MASTER STATUS"
echo -e "${GREEN}✓ Binlog status checked${NC}"
echo ""

# 4. GTID 확인
echo -e "${BLUE}[4/5] Checking GTID status...${NC}"
docker exec rust_mysql_cdc_test mysql -u root -prootpassword testdb -e "SELECT @@GLOBAL.GTID_MODE as GTID_MODE, @@GLOBAL.GTID_EXECUTED as GTID_EXECUTED;"
echo -e "${GREEN}✓ GTID status checked${NC}"
echo ""

# 5. 데이터 확인
echo -e "${BLUE}[5/5] Checking test data...${NC}"
echo ""
echo "Users table:"
docker exec rust_mysql_cdc_test mysql -u root -prootpassword testdb -e "SELECT * FROM users;"
echo ""
echo "Orders table:"
docker exec rust_mysql_cdc_test mysql -u root -prootpassword testdb -e "SELECT * FROM orders;"
echo ""
echo "Order items table:"
docker exec rust_mysql_cdc_test mysql -u root -prootpassword testdb -e "SELECT * FROM order_items;"
echo ""
echo -e "${GREEN}✓ Test data verified${NC}"
echo ""

echo "====================================="
echo -e "${GREEN}MySQL Test Setup Complete!${NC}"
echo "====================================="
echo ""
echo -e "${YELLOW}Now run the CDC engine:${NC}"
echo "  DB_HOST=localhost \\"
echo "  DB_PORT=3306 \\"
echo "  DB_USER=testuser \\"
echo "  DB_PASSWORD=testpass \\"
echo "  DB_NAME=testdb \\"
echo "  cargo run"
echo ""
echo -e "${YELLOW}Or run tests:${NC}"
echo "  cargo test"
echo ""
echo -e "${YELLOW}Admin Panel:${NC}"
echo "  http://localhost:8080"
echo ""
echo -e "${YELLOW}To stop containers:${NC}"
echo "  docker-compose down"
echo ""
