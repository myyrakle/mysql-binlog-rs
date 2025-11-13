-- MySQL CDC 테스트용 초기화 스크립트

-- 테스트 데이터베이스 생성
CREATE DATABASE IF NOT EXISTS testdb;
USE testdb;

-- 사용자 테이블
CREATE TABLE users (
  id INT PRIMARY KEY AUTO_INCREMENT,
  username VARCHAR(255) NOT NULL UNIQUE,
  email VARCHAR(255) NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 주문 테이블
CREATE TABLE orders (
  id INT PRIMARY KEY AUTO_INCREMENT,
  user_id INT NOT NULL,
  total_amount DECIMAL(10, 2) NOT NULL,
  status VARCHAR(50) DEFAULT 'pending',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
  INDEX idx_user_id (user_id),
  INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 주문 상품 테이블
CREATE TABLE order_items (
  id INT PRIMARY KEY AUTO_INCREMENT,
  order_id INT NOT NULL,
  product_name VARCHAR(255) NOT NULL,
  quantity INT NOT NULL,
  unit_price DECIMAL(10, 2) NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE,
  INDEX idx_order_id (order_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 테스트 데이터 삽입
INSERT INTO users (username, email) VALUES
('alice', 'alice@example.com'),
('bob', 'bob@example.com'),
('charlie', 'charlie@example.com');

INSERT INTO orders (user_id, total_amount, status) VALUES
(1, 150.00, 'pending'),
(2, 250.50, 'completed'),
(1, 75.25, 'shipped');

INSERT INTO order_items (order_id, product_name, quantity, unit_price) VALUES
(1, 'Laptop', 1, 150.00),
(2, 'Mouse', 5, 25.10),
(2, 'Keyboard', 3, 50.00),
(3, 'USB Cable', 3, 25.25);

-- CDC 테스트용 권한 설정
GRANT SELECT, REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'testuser'@'%';
FLUSH PRIVILEGES;
