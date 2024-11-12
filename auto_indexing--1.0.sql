CREATE FUNCTION auto_indexing() RETURNS void
AS 'MODULE_PATHNAME', 'auto_indexing'
    LANGUAGE C STRICT;

SELECT auto_indexing();

COMMENT ON EXTENSION auto_indexing IS 'Auto Indexing extension for Postgresql. Project for IIT Bombay, CS631.';
