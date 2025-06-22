-- CREAET PACKAGE:


create or replace PACKAGE PKG_GLOBAL_LOG
AS
 PROCEDURE P_GLOBAL_LOG;
  gv_run_id         INTEGER;
  gv_table_name     VARCHAR2(50 CHAR);
  gv_schema_name    VARCHAR2(50 CHAR);
  gv_log_text       VARCHAR2(4000 CHAR);
  gv_procedure_name VARCHAR2(500 CHAR);
  gv_operation      VARCHAR2(50 CHAR);
  gv_affected_rows  NUMBER(38,0) := 0;
  gv_start_date     TIMESTAMP := CURRENT_TIMESTAMP;
  gv_end_date       TIMESTAMP := CURRENT_TIMESTAMP;
  gv_exception_text VARCHAR2(4000 CHAR) := '';
END PKG_GLOBAL_LOG;




-- CREATE PACKAGE BODY:

create or replace PACKAGE BODY PKG_GLOBAL_LOG
AS
  
  
  PROCEDURE P_GLOBAL_LOG
  IS
    PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
  IF PKG_GLOBAL_LOG.gv_run_id IS NULL THEN
    SELECT GLOBAL_LOG_ID.NEXTVAL INTO PKG_GLOBAL_LOG.gv_run_id FROM DUAL;
  END IF;

  INSERT INTO ETL_GLOBAL_LOG
    ( 
    USER_NAME, 
    HOST_NAME,
    RUN_ID, 
    ROW_NUM, 
    PROCEDURE_NAME,
    SCHEMA_NAME,
    TABLE_NAME, 
    LOG_TEXT, 
    OPERATION, 
    AFFECTED_ROWS, 
    START_DATE, 
    END_DATE, 
    EXECUTION_TIME, 
    STATUS, 
    EXCEPTION_TEXT)
  SELECT 
    SYS_CONTEXT('USERENV', 'OS_USER'),
    SYS_CONTEXT('USERENV', 'HOST'),
    PKG_ETL_GLOBAL_LOG.gv_run_id,
    NVL (MAX (row_num) + 1, 1),
    REGEXP_SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK, 'procedure\s+([^[:cntrl:]]+)',1,1,NULL,1),    
    gv_schema_name,
    gv_table_name,
    gv_log_text,
    gv_operation,
    gv_affected_rows,
    gv_start_date,
    gv_end_date,
    gv_end_date - gv_start_date,
    CASE
        WHEN PKG_GLOBAL_LOG.gv_exception_text IS NULL THEN 'OK'
        ELSE 'NOT OK'
    END,
    gv_exception_text
  FROM ETL_GLOBAL_LOG
  WHERE run_id            = PKG_GLOBAL_LOG.gv_run_id
  AND NVL(TABLE_NAME,'*') = NVL(PKG_GLOBAL_LOG.gv_table_name,'*');
  COMMIT;
END P_GLOBAL_LOG;
end PKG_GLOBAL_LOG;