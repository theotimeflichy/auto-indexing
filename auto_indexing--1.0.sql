CREATE FUNCTION auto_indexing() RETURNS void AS 'auto_indexing' LANGUAGE C STRICT;
CREATE FUNCTION audit(int) RETURNS void AS 'auto_indexing' LANGUAGE C STRICT;
CREATE FUNCTION audit_end() RETURNS void AS 'auto_indexing' LANGUAGE C STRICT;

SELECT auto_indexing();

COMMENT ON EXTENSION auto_indexing IS 'Auto Indexing extension for Postgresql. Project for IIT Bombay, CS631.';

CREATE TABLE query_log (
    query_type VARCHAR(50) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    clause_type VARCHAR(50),
    clause_name TEXT,
    log_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);