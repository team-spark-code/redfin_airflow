CREATE DATABASE IF NOT EXISTS redfin CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
USE redfin;

CREATE TABLE IF NOT EXISTS jsonl_ingest (
  id            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  -- 원본 추적
  src_file      VARCHAR(512) NOT NULL,                 -- jsonl 파일 경로 또는 논리 ID
  file_sha256   CHAR(64) NOT NULL,
  line_no       INT UNSIGNED NOT NULL,
  ingested_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

  -- 원본 그대로
  raw_json      JSON NOT NULL,                         -- MariaDB는 내부적으로 LONGTEXT+JSON_VALID

  -- 자주 쓰는 정규화 필드(필요한 것만 선택적으로 저장)
  guid          VARCHAR(255) NULL,
  source        VARCHAR(512) NULL,
  title         TEXT NULL,
  link          TEXT NULL,
  link_hash     CHAR(64) AS (SHA2(link, 256)) STORED,
  pub_date      DATETIME NULL,
  author        JSON NULL,
  summary       MEDIUMTEXT NULL,
  content_text  MEDIUMTEXT NULL,

  PRIMARY KEY (id),

  -- 같은 파일 동일 라인 재적재 방지
  UNIQUE KEY uq_src_line (file_sha256, line_no),

  -- 대체 유니크 키(있을 때만): guid 또는 link_hash
  KEY idx_guid (guid),
  KEY idx_link_hash (link_hash),

  -- 조회/정렬 인덱스
  KEY idx_source_date (source(191), pub_date),
  KEY idx_ingested (ingested_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
