create or replace PROCEDURE P_DATA_UPDATE
is
BEGIN 
        SCHEMA.PKG_GLOBAL_LOG.gv_table_name := 'DATA';
        SCHEMA.PKG_GLOBAL_LOG.gv_schema_name := 'SCHEMA';
        SCHEMA.PKG_GLOBAL_LOG.gv_log_text := 'Procedure P_DATA_UPDATE was start';
        SCHEMA.PKG_GLOBAL_LOG.gv_start_date := CURRENT_TIMESTAMP;
        SCHEMA.PKG_GLOBAL_LOG.P_GLOBAL_LOG;



-- Load main dataset 
                MERGE INTO "SCHEMA"."DATA" t
                USING (
                SELECT * FROM(
                SELECT TENDER_ID, TENDER_NUMBER, TENDER_NAME, BEGIN_PRICE, CUSTOMER_NAME, REGION, PUBLISH_DATE, START_DATE, END_DATE, TENDER_TYPE, SOURSE_LINK, LOT_CATEGORY, LOT_DELIVERY_PLACE, LOT_KTRU_NAME, CUSTOMER_INN, UPDATE_DATE, AUTOSEARCH_ID,
                ROW_NUMBER() OVER (PARTITION BY TENDER_ID ORDER BY TENDER_ID DESC) AS rn
                FROM "SCHEMA"."STG_DATA")
                WHERE rn = 1
                ) s
                ON (t.TENDER_ID = s.TENDER_ID)
                WHEN MATCHED THEN
                     UPDATE SET
                        t.TENDER_NUMBER = s.TENDER_NUMBER,
                        t.TENDER_NAME = s.TENDER_NAME, 
                        t.BEGIN_PRICE = TO_NUMBER(REPLACE(s.BEGIN_PRICE, '.', ','), '999999999999D99', 'NLS_NUMERIC_CHARACTERS='',.'''), 
                        t.CUSTOMER_NAME = s.CUSTOMER_NAME, 
                        t.REGION = s.REGION, 
                        t.PUBLISH_DATE = s.PUBLISH_DATE, 
                        t.START_DATE = s.START_DATE, 
                        t.END_DATE = s.END_DATE, 
                        t.TENDER_TYPE = s.TENDER_TYPE, 
                        t.SOURSE_LINK = s.SOURSE_LINK,
                        t.LOT_CATEGORY = s.LOT_CATEGORY, 
                        t.LOT_DELIVERY_PLACE = s.LOT_DELIVERY_PLACE, 
                        t.LOT_KTRU_NAME = s.LOT_KTRU_NAME, 
                        t.CUSTOMER_INN = s.CUSTOMER_INN, 
                        --t."SAP ID" = '',
                        t.AUTOSEARCH_ID = s.AUTOSEARCH_ID,
                        t.IS_ACTIVE = 1
                WHEN NOT MATCHED THEN INSERT (
                    t.TENDER_ID,
                    t.TENDER_NUMBER,
                    t.TENDER_NAME,
                    t.BEGIN_PRICE,
                    t.CUSTOMER_NAME,
                    t.REGION,
                    t.PUBLISH_DATE,
                    t.START_DATE,
                    t.END_DATE,
                    t.TENDER_TYPE,
                    t.SOURSE_LINK,
                    t.LOT_CATEGORY,
                    t.LOT_DELIVERY_PLACE,
                    t.LOT_KTRU_NAME,
                    t.CUSTOMER_INN,
                    t."SAP ID",
                    t.AUTOSEARCH_ID,
                    t.IS_ACTIVE,
                    t.UPDATE_DATE
                )
                   VALUES (
                    s.TENDER_ID,
                    s.TENDER_NUMBER, 
                    s.TENDER_NAME, 
                    TO_NUMBER(REPLACE(s.BEGIN_PRICE, '.', ','), '999999999999D99', 'NLS_NUMERIC_CHARACTERS='',.'''), 
                    s.CUSTOMER_NAME, 
                    s.REGION, 
                    s.PUBLISH_DATE, 
                    s.START_DATE, 
                    s.END_DATE, 
                    s.TENDER_TYPE, 
                    s.SOURSE_LINK,
                    s.LOT_CATEGORY, 
                    s.LOT_DELIVERY_PLACE, 
                    s.LOT_KTRU_NAME, 
                    s.CUSTOMER_INN, 
                    '',
                    '',
                    1,
                    CURRENT_TIMESTAMP
                   );
                   
                   SCHEMA.PKG_GLOBAL_LOG.gv_log_text := 'Merge data from SCHEMA_01.STG_DATA into SCHEMA.DATA complit';
                   SCHEMA.PKG_GLOBAL_LOG.gv_operation := 'MERGE';
                   SCHEMA.PKG_GLOBAL_LOG.gv_affected_rows := SQL%ROWCOUNT;
                   SCHEMA.PKG_GLOBAL_LOG.gv_end_date := CURRENT_TIMESTAMP;
                   SCHEMA.PKG_GLOBAL_LOG.P_GLOBAL_LOG;
                   
                   
                   

COMMIT;
-- Load autosearch list into column AUTOSEARCH_ID
                MERGE INTO "SCHEMA"."DATA" t
                USING ( SELECT TENDER_ID, LISTAGG(AUTOSEARCH_ID, ', ') WITHIN GROUP (ORDER BY AUTOSEARCH_ID) AS AUTOSEARCH_ID, LISTAGG(BU_NAME, ', ') WITHIN GROUP (ORDER BY AUTOSEARCH_ID) AS BU_NAME
                        FROM (SELECT stg.TENDER_ID, stg.AUTOSEARCH_ID, bun.BU_NAME
                                FROM "SCHEMA_01"."STG_DATA" stg
                                LEFT JOIN "SCHEMA"."MKT_SET_BU_NAME" bun ON stg.AUTOSEARCH_ID = bun.AUTOSEARCH_ID)
                        GROUP BY TENDER_ID)  s
                ON (t.TENDER_ID = s.TENDER_ID)
                WHEN MATCHED THEN
                    UPDATE SET t.AUTOSEARCH_ID = s.AUTOSEARCH_ID, t.BU_NAME = s.BU_NAME;
                    
COMMIT;   
-- Load SAP ID    
                MERGE INTO "SCHEMA"."DATA" t
                USING (SELECT "STPS ID", "SAP ID" FROM(SELECT "STPS ID", "SAP ID", ROW_NUMBER() OVER (PARTITION BY "STPS ID" ORDER BY "STPS ID" DESC) AS rn FROM "SCHEMA"."TPS_TPS_CUSTOMER_MATCH" WHERE NAME LIKE '0%') WHERE rn = 1)  s
                ON (t.CUSTOMER_INN = s."STPS ID")
                WHEN MATCHED THEN
                    UPDATE SET t."SAP ID" = s."SAP ID";
    
COMMIT;
-- Set is active = 0 for closed tenders
                UPDATE "SCHEMA"."DATA" t
                SET t.IS_ACTIVE = 0,
                    t.UPDATE_DATE = CURRENT_TIMESTAMP
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM "SCHEMA_01"."STG_DATA" s
                    WHERE s.TENDER_ID = t.TENDER_ID
                ) AND t.IS_ACTIVE = 1;
                
                
                   SCHEMA.PKG_GLOBAL_LOG.gv_log_text := 'Set IS_ACTIVE = 0 for closed tenders complete';
                   SCHEMA.PKG_GLOBAL_LOG.gv_operation := 'UPDATE';
                   SCHEMA.PKG_GLOBAL_LOG.gv_affected_rows := SQL%ROWCOUNT;
                   SCHEMA.PKG_GLOBAL_LOG.gv_end_date := CURRENT_TIMESTAMP;
                   SCHEMA.PKG_GLOBAL_LOG.P_GLOBAL_LOG;
                
COMMIT;
-- Set "UPDATE_DATE" = current_timestamp for updated rows (updated in last 24 hours)

                UPDATE "SCHEMA"."DATA" t
                SET t.UPDATE_DATE = CURRENT_TIMESTAMP
                WHERE t.TENDER_ID IN (SELECT TENDER_ID FROM "SCHEMA_01"."STG_DATA"
                                      WHERE UPDATE_DATE != '0001-01-01T00:00:00+03:00' AND TO_TIMESTAMP_TZ(UPDATE_DATE, 'YYYY-MM-DD"T"HH24:MI:SS TZH:TZM') >= CURRENT_TIMESTAMP - INTERVAL '1' DAY - INTERVAL '7' HOUR
                                      GROUP BY TENDER_ID);

                   SCHEMA.PKG_GLOBAL_LOG.gv_log_text := 'Set CURRENT_TIMESTAMP into UPDATE_DATE column for tendet that have changes in the last 24 hours';
                   SCHEMA.PKG_GLOBAL_LOG.gv_operation := 'UPDATE';
                   SCHEMA.PKG_GLOBAL_LOG.gv_affected_rows := SQL%ROWCOUNT;
                   SCHEMA.PKG_GLOBAL_LOG.gv_end_date := CURRENT_TIMESTAMP;
                   SCHEMA.PKG_GLOBAL_LOG.P_GLOBAL_LOG;


                   SCHEMA.PKG_GLOBAL_LOG.gv_log_text := 'Procedure P_DATA_UPDATE has been successfully completed';
                   SCHEMA.PKG_GLOBAL_LOG.gv_operation := '';
                   SCHEMA.PKG_GLOBAL_LOG.gv_affected_rows := NULL;
                   SCHEMA.PKG_GLOBAL_LOG.gv_end_date := CURRENT_TIMESTAMP;
                   SCHEMA.PKG_GLOBAL_LOG.P_GLOBAL_LOG;
                                     
COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        SCHEMA.PKG_GLOBAL_LOG.gv_log_text := 'ERROR';
        SCHEMA.PKG_GLOBAL_LOG.gv_exception_text := SQLERRM;
        SCHEMA.PKG_GLOBAL_LOG.gv_end_date := CURRENT_TIMESTAMP;
        SCHEMA.PKG_GLOBAL_LOG.P_GLOBAL_LOG;

END;