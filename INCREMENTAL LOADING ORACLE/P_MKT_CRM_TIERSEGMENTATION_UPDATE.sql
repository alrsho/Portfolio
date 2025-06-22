create or replace procedure p_DATA
is
        v_stg_count_rows number;
begin
-- Start log
        SCHEMA_01.PKG_GLOBAL_LOG.gv_table_name := 'DATA';
        SCHEMA_01.PKG_GLOBAL_LOG.gv_schema_name := 'SCHEMA';
        SCHEMA_01.PKG_GLOBAL_LOG.gv_log_text := 'Procedure P_DATA was start';
        SCHEMA_01.PKG_GLOBAL_LOG.gv_start_date := CURRENT_TIMESTAMP;
        SCHEMA_01.PKG_GLOBAL_LOG.P_GLOBAL_LOG;







-- Reload data in actual STG_DATA
    execute immediate 'truncate table STG_DATA';
    
    insert --+ append
    into STG_DATA
    select
        SAP_ID,
        TIER_ABC,
        Penetration,
        PLATFORM,
        SEGMENT,
        MS_LEVEL,
        '' as HASH_MD5
    from SHEMA_02.file_data;
    
    commit;
    -- LOG
    SCHEMA_01.PKG_GLOBAL_LOG.gv_log_text := 'Insert data in SCHEMA.STG_DATA complete';
    SCHEMA_01.PKG_GLOBAL_LOG.gv_operation := 'INSERT';
    select count(*) into SCHEMA_01.PKG_GLOBAL_LOG.gv_affected_rows from STG_DATA ;
    SCHEMA_01.PKG_GLOBAL_LOG.gv_end_date := CURRENT_TIMESTAMP;
    SCHEMA_01.PKG_GLOBAL_LOG.P_GLOBAL_LOG;
    
    
    
    
    
 -- Calculate HASH values   
    p_hash_calc('STG_DATA');

-- UPDATE ROWS  
        MERGE INTO DATA
            USING 
            (
               select STG_DATA.* from STG_DATA
               where exists (
                    select 1 from DATA 
                    where
                        STG_DATA.SAP_ID = DATA.SAP_ID
                        and STG_DATA.PLATFORM = DATA.PLATFORM
                        and STG_DATA.HASH_MD5 <> DATA.HASH_MD5
                        and DATA.OPERATION <> 'DELETED')
            ) STG_DATA 
            ON (STG_DATA.SAP_ID = DATA .SAP_ID and STG_DATA.PLATFORM = DATA .PLATFORM)
          WHEN MATCHED THEN
            UPDATE SET
            
                DATA.TIER_ABC = STG_DATA.TIER_ABC,
                DATA.PENETRATION = STG_DATA.PENETRATION,
                DATA.SEGMENT = STG_DATA.SEGMENT,
                DATA.MS_LEVEL = STG_DATA.MS_LEVEL,
                DATA.HASH_MD5 = STG_DATA.HASH_MD5,
                DATA.OPERATION = 'UPDATE',
                DATA.SYNCDATE = CURRENT_DATE;
                
                -- LOG  
                SCHEMA_01.PKG_GLOBAL_LOG.gv_log_text := 'UPDATE rows in DATA';
                SCHEMA_01.PKG_GLOBAL_LOG.gv_operation := 'MERGE';
                SCHEMA_01.PKG_GLOBAL_LOG.gv_affected_rows := SQL%ROWCOUNT;
                SCHEMA_01.PKG_GLOBAL_LOG.gv_end_date := CURRENT_TIMESTAMP;
                SCHEMA_01.PKG_GLOBAL_LOG.P_GLOBAL_LOG;               
                
    commit
    ;
    
    
    
    
    
 -- INSERT NEW ROWS   
    insert into DATA
    select 
        STG_DATA.*, CURRENT_DATE AS SYNCDATE, 'INSERT' AS OPERATION
    from
        STG_DATA
    where not exists (select 1 from DATA where STG_DATA.SAP_ID = DATA.SAP_ID
                                                                and STG_DATA.PLATFORM = DATA.PLATFORM 
                                                                and DATA.OPERATION <> 'DELETED')
    ;
    
    
                -- LOG
                SCHEMA_01.PKG_GLOBAL_LOG.gv_log_text := 'INSERT NEW rows into DATA';
                SCHEMA_01.PKG_GLOBAL_LOG.gv_operation := 'INSERT';
                SCHEMA_01.PKG_GLOBAL_LOG.gv_affected_rows := SQL%ROWCOUNT;
                SCHEMA_01.PKG_GLOBAL_LOG.gv_end_date := CURRENT_TIMESTAMP;
                SCHEMA_01.PKG_GLOBAL_LOG.P_GLOBAL_LOG;        

    commit
    ;
    
-- Set 'DELETED' flg for deleted rows
    select count(*) into v_stg_count_rows from STG_DATA
    ;
    IF v_stg_count_rows>0 then
        update DATA
        set 
            OPERATION = 'DELETED', SYNCDATE = CURRENT_DATE
        where
            (SAP_ID, PLATFORM)  not in (select SAP_ID, PLATFORM from STG_DATA)
            and DATA.OPERATION <> 'DELETED'
        ;
                    -- LOG
                    SCHEMA_01.PKG_GLOBAL_LOG.gv_log_text := 'SET DELETED flag for deleted rows in DATA';
                   SCHEMA_01.PKG_GLOBAL_LOG.gv_operation := 'UPDATE';
                   SCHEMA_01.PKG_GLOBAL_LOG.gv_affected_rows := SQL%ROWCOUNT;
                   SCHEMA_01.PKG_GLOBAL_LOG.gv_end_date := CURRENT_TIMESTAMP;
                   SCHEMA_01.PKG_GLOBAL_LOG.P_GLOBAL_LOG;     

        commit
        ;
    end if;
    
 -- Finish log
                    SCHEMA_01.PKG_GLOBAL_LOG.gv_log_text := 'Procedure P_DATA has been successfully completed';
                   SCHEMA_01.PKG_GLOBAL_LOG.gv_operation := '';
                   SCHEMA_01.PKG_GLOBAL_LOG.gv_affected_rows := NULL;
                   SCHEMA_01.PKG_GLOBAL_LOG.gv_end_date := CURRENT_TIMESTAMP;
                   SCHEMA_01.PKG_GLOBAL_LOG.P_GLOBAL_LOG;
    
EXCEPTION
    WHEN OTHERS THEN
        SCHEMA_01.PKG_GLOBAL_LOG.gv_log_text := 'ERROR';
        SCHEMA_01.PKG_GLOBAL_LOG.gv_exception_text := SQLERRM;
        SCHEMA_01.PKG_GLOBAL_LOG.gv_end_date := CURRENT_TIMESTAMP;
        SCHEMA_01.PKG_GLOBAL_LOG.P_GLOBAL_LOG;  
    
    
    
    
    
    
    
    
    
end;