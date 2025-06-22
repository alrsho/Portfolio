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


-- Reload data in actualSTG_DATA
    execute immediate 'truncate tableSTG_DATA';
    insert --+ append
    intoSTG_DATA
    select distinct
    lower(NETWORKACCOUNT) as USERNAME,
    EMPLOYEE as NAME,
    MANAGER as MANAGERNAME,
    SAP_ID,
    lpu_inn.STCD1 as INN,
    lpu_cust_name.CUSTOMER_NAME as CUST_NAME,
    Sales_Team_Name as SALES_TEAM_NAME,
    VALID_FROM,
    VALID_TO,
    '' AS HASH_MD5
from
    (SELECT * 
     FROM(
        SELECT
        tps_msc_links.*,
        MAX(LINK_ID) OVER (PARTITION BY SALES_TEAM_NAME, SAP_ID, lower(NETWORKACCOUNT), VALID_FROM) AS max_link_id
        FROM tps_msc_links)
    WHERE max_link_id = link_id) tps_msc_links
    join (select kunnr, STCD1 from schema_03.kna1) lpu_inn
        on lpu_inn.kunnr = tps_msc_links.SAP_ID
    join (select distinct rubi_sap_id, CUSTOMER_NAME from schema_02.table_02) lpu_cust_name
        on lpu_cust_name.rubi_sap_id = tps_msc_links.SAP_ID;
    
    
    commit;
    -- LOG
    SCHEMA_01.PKG_GLOBAL_LOG.gv_log_text := 'Insert data in SCHEMA.MKT_CRM_STG_LINK_MANAGER complete';
    SCHEMA_01.PKG_GLOBAL_LOG.gv_operation := 'INSERT';
    select count(*) into SCHEMA_01.PKG_GLOBAL_LOG.gv_affected_rows fromSTG_DATA ;
    SCHEMA_01.PKG_GLOBAL_LOG.gv_end_date := CURRENT_TIMESTAMP;
    SCHEMA_01.PKG_GLOBAL_LOG.P_GLOBAL_LOG;
    
    
    
    
    
 -- Calculate HASH values   
    p_hash_calc('MKT_CRM_STG_LINK_MANAGER');

-- UPDATE ROWS  
        MERGE INTO DATA
            USING 
            (
               selectSTG_DATA.* fromSTG_DATA
               where exists (
                    select 1 from DATA 
                    where
                       STG_DATA.SAP_ID = DATA.SAP_ID
                        andSTG_DATA.USERNAME = DATA.USERNAME
                        andSTG_DATA.SALES_TEAM_NAME = DATA.SALES_TEAM_NAME
                        andSTG_DATA.VALID_FROM = DATA.VALID_FROM
                        andSTG_DATA.HASH_MD5 <> DATA.HASH_MD5
                        and DATA.OPERATION <> 'DELETED')
            )STG_DATA 
            ON (   STG_DATA.SAP_ID = DATA.SAP_ID 
                andSTG_DATA.USERNAME = DATA.USERNAME
                andSTG_DATA.SALES_TEAM_NAME = DATA.SALES_TEAM_NAME
                andSTG_DATA.VALID_FROM = DATA.VALID_FROM)
          WHEN MATCHED THEN
            UPDATE SET
            
                DATA.NAME =STG_DATA.NAME,
                DATA.MANAGERNAME =STG_DATA.MANAGERNAME,
                DATA.INN =STG_DATA.INN,
                DATA.CUST_NAME =STG_DATA.CUST_NAME,
                DATA.VALID_TO =STG_DATA.VALID_TO,
                DATA.HASH_MD5 =STG_DATA.HASH_MD5,
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
    where not exists (select 1 from DATA whereSTG_DATA.SAP_ID = DATA.SAP_ID
                                                                andSTG_DATA.USERNAME = DATA.USERNAME
                                                                andSTG_DATA.SALES_TEAM_NAME = DATA.SALES_TEAM_NAME 
                                                                andSTG_DATA.VALID_FROM = DATA.VALID_FROM 
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
    select count(*) into v_stg_count_rows fromSTG_DATA
    ;
    IF v_stg_count_rows>0 then
        update DATA
        set 
            OPERATION = 'DELETED', SYNCDATE = CURRENT_DATE
        where
            (SAP_ID, USERNAME, SALES_TEAM_NAME, VALID_FROM)  not in (select SAP_ID, USERNAME, SALES_TEAM_NAME, VALID_FROM fromSTG_DATA)
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
                    SCHEMA_01.PKG_GLOBAL_LOG.gv_log_text := 'Procedure P_MKT_CRM_STG_LINK_MANAGER has been successfully completed';
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