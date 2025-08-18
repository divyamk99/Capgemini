CREATE OR REPLACE PROCEDURE DAAS_COMMON.TRIP_COMP_DOMAIN_PIPELINE_PROC(BATCH_ID FLOAT, SHARD_NAME VARCHAR, WAREHOUSE_NAME VARCHAR, CUSTOM_PARAM1 VARCHAR, CUSTOM_PARAM2 VARCHAR) 
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER AS
$$			
/*
#####################################################################################
Author: Ram Modepalli
Purpose: Build Comp Domain Pipeline for Trip
Input Parameters: BATCH_ID, SHARD_NAME, WAREHOUSE_NAME, CUSTOM_PARAM1, CUSTOM_PARAM2
Output Value: SUCCESS for successful execution and for failed execution it returns sql statement causing issue with error description
Created Date: 09/16/2022 
Version: 1.0
Modified By: Surya Jangareddi
Modified Date: 12/15/2023
Version: 1.2 [Added filter TD.DELETE_IND = 'N' at my_sql_command_3 to handle if any existing txn got update and this is getting marked 
			  as DELETE_IND = N and TRANSACTION_SUB_TYPE as VOIDED]
#####################################################################################
*/

proc_output = "";
proc_step = "";
			
snowflake.execute( {sqlText: "USE WAREHOUSE " + WAREHOUSE_NAME} );
tag = BATCH_ID + "_COMP_DOMAIN_PIPELINE_PROC";
snowflake.execute( {sqlText: "ALTER SESSION SET QUERY_TAG = '" + tag + "'" });

ddl_text = `
CREATE OR REPLACE TABLE DAAS_TEMP.COMP_DETAIL_FACT_TRIP
(
	COMP_DETAIL_FACT_SK BIGINT,
	GUEST_UNIQUE_ID INTEGER,
	PROPERTY_CD VARCHAR(3),
	TRANSACTION_DTTM TIMESTAMP_NTZ(9),
	COMP_STATUS_CD CHAR(1),
	DELETE_IND CHAR(1),
	UPDATED_DTTM TIMESTAMP_NTZ(9)
) 
`;

snowflake.execute( {sqlText: ddl_text});

ddl_text = `
CREATE OR REPLACE TABLE DAAS_TEMP.TRIP_RESULT_COMP_ALL_TRIP_TYPES
(
	GUEST_UNIQUE_ID INTEGER,
	PROPERTY_CD VARCHAR(3),
	TRIP_TYPE VARCHAR(16),
	MARKET_CD VARCHAR(4),
	TRANSACTION_TABLE_SK BIGINT,
	TRANSACTION_TABLE VARCHAR(128),
	TRANSACTION_TYPE VARCHAR(16),
	TRANSACTION_START_DTTM TIMESTAMP_NTZ(9),
	TRANSACTION_END_DTTM TIMESTAMP_NTZ(9),
	BUSINESS_START_DT DATE,
	BUSINESS_END_DT DATE,
	TRANSACTION_MODIFIED_DTTM TIMESTAMP_NTZ(9)
) 
`;

snowflake.execute( {sqlText: ddl_text});

ddl_text = `
CREATE OR REPLACE TABLE DAAS_TEMP.TRIP_RESULT_COMP
(
	GUEST_UNIQUE_ID INTEGER,
	PROPERTY_CD VARCHAR(3),
	TRIP_TYPE VARCHAR(16),
	MARKET_CD VARCHAR(4),
	TRANSACTION_TABLE_SK BIGINT,
	TRANSACTION_TABLE VARCHAR(128),
	TRANSACTION_TYPE VARCHAR(16),
	TRANSACTION_START_DTTM TIMESTAMP_NTZ(9),
	TRANSACTION_END_DTTM TIMESTAMP_NTZ(9),
	BUSINESS_START_DT DATE,
	BUSINESS_END_DT DATE,
	TRANSACTION_MODIFIED_DTTM TIMESTAMP_NTZ(9),
	TRIP_INDICATOR VARCHAR(64)
) 
`;

snowflake.execute( {sqlText: ddl_text});

ddl_text = `
CREATE OR REPLACE TABLE DAAS_TEMP.TRIP_RANGE_COMP
(
	GUEST_UNIQUE_ID INTEGER,
	PROPERTY_CD VARCHAR(3),
	TRIP_TYPE VARCHAR(16),
	MARKET_CD VARCHAR(4),
	TRANSACTION_TABLE_SK BIGINT,
	TRANSACTION_TABLE VARCHAR(128),
	TRANSACTION_TYPE VARCHAR(16),
	TRANSACTION_START_DTTM TIMESTAMP_NTZ(9),
	TRANSACTION_END_DTTM TIMESTAMP_NTZ(9),
	BUSINESS_START_DT DATE,
	BUSINESS_END_DT DATE,
	TRANSACTION_MODIFIED_DTTM TIMESTAMP_NTZ(9),
	TRIP_INDICATOR VARCHAR(64),
	MAPPING_STATUS VARCHAR(16),
	TRIP_START_DT DATE,
	TRIP_END_DT DATE
) 
`;

snowflake.execute( {sqlText: ddl_text});

/*
	Trip open criteria :- 
	COMP_STATUS_CD = R -> Consider all transactions 
	COMP_STATUS_CD = M -> Consider transactions with either USED_COMP_WORTH_AMT > 0 OR USED_REWARD_CREDT_AMT > 0 
*/

my_sql_command_1 = `
INSERT INTO DAAS_TEMP.COMP_DETAIL_FACT_TRIP
SELECT DISTINCT
	CDFVW.COMP_DETAIL_FACT_SK,
	CDFVW.GUEST_UNIQUE_ID,
	CDFVW.PROPERTY_CD,
	CDFVW.REDEEM_DTTM,
	CDFVW.COMP_STATUS_CD,
	CASE WHEN CONTAINS(VD.VALUE, CDF.COMP_STATUS_CD) THEN 'Y' ELSE CDFVW.DELETE_IND END,
	CDFVW.UPDATED_DTTM
FROM
	DAAS_CORE.COMP_DETAIL_FACT_COMP_DOMAIN_STREAM CDF
JOIN
	DAAS_CORE_GAMING_VW.COMP_DETAIL_FACT_VW CDFVW
ON
	CDFVW.COMP_DETAIL_FACT_SK = CDF.COMP_DETAIL_FACT_SK
JOIN
	TABLE(DAAS_COMMON.TRIP_CONFIG_UDF('COMP_STATUS_CD', 'TRIP_TRIGGER', '', '', 'COMP_OPEN_TRANSACTION', CDF.REDEEM_DTTM::DATE)) TTC
JOIN
	TABLE(DAAS_COMMON.TRIP_CONFIG_UDF('COMP_STATUS_CD', 'TRIP_TRIGGER', '', '', 'COMP_VOID_TRANSACTION', CDF.REDEEM_DTTM::DATE)) VD
WHERE
	(CONTAINS(TTC.VALUE, CDF.COMP_STATUS_CD) OR CONTAINS(VD.VALUE, CDF.COMP_STATUS_CD))
	AND CASE WHEN CDF.COMP_STATUS_CD = 'M' AND COALESCE(CDF.USED_COMP_WORTH_AMT, 0) = 0 AND COALESCE(CDF.USED_REWARD_CREDT_AMT, 0) = 0 THEN FALSE ELSE TRUE END 
	--AND CDFVW.GUEST_UNIQUE_ID <> 0	
	AND METADATA$ACTION = 'INSERT'
	AND EXISTS 
	(
		SELECT
			1
		FROM
			DAAS_CORE.TRIP_CONFIG TC
		WHERE
			TC.PROPERTY_CD = CDFVW.PROPERTY_CD
			AND TC.ACTIVE_FLG = 'Y'
			AND EXISTS 
			(
				SELECT
					1
				FROM
					DAAS_COMMON.PROPERTY_MARKET_LKP LKP
				WHERE
					TC.PROPERTY_CD = LKP.PROPERTY_CD 
			) 
	)
`;

my_sql_command_1_1 = `
INSERT INTO DAAS_CORE.TRIP_FILTERED_TXNS(TRANSACTION_TABLE_SK, TRANSACTION_TYPE, BATCH_ID)
SELECT DISTINCT
	COMP_DETAIL_FACT_SK,
	'COMP' AS TRANSACTION_TYPE,
	` + BATCH_ID + ` AS BATCH_ID
FROM
	DAAS_TEMP.COMP_DETAIL_FACT_TRIP CDFT
WHERE
	GUEST_UNIQUE_ID = 0
	AND NOT EXISTS
	(
		SELECT
			1
		FROM
			DAAS_CORE.TRIP_FILTERED_TXNS TFT
		WHERE
			CDFT.COMP_DETAIL_FACT_SK = TFT.TRANSACTION_TABLE_SK
			AND TFT.TRANSACTION_TYPE = 'COMP'
			AND TFT.DELETE_IND = 'N'
	)
`;

my_sql_command_1_2 = `DELETE FROM DAAS_TEMP.COMP_DETAIL_FACT_TRIP WHERE GUEST_UNIQUE_ID = 0`;

/*
	-- COMP_STATUS_CD = V - Soft delete the corresponding transaction in Trip Detail
	-- Added logic to handle the change in transaction_dttm for the same transaction 
*/

my_sql_command_2 =`
UPDATE DAAS_CORE.TRIP_DETAIL TD
SET
	TD.DELETE_IND 					= CASE WHEN DATE(CDF.TRANSACTION_DTTM) <> TD.BUSINESS_START_DT THEN 'Y' ELSE CDF.DELETE_IND END,
	TD.TRANSACTION_SUB_TYPE 		= CASE WHEN CDF.DELETE_IND = 'Y' THEN 'VOIDED' WHEN DATE(CDF.TRANSACTION_DTTM) <> TD.BUSINESS_START_DT THEN 'TRANSACTION CHANGED' ELSE TD.TRANSACTION_SUB_TYPE END,
	TD.TRANSACTION_MODIFIED_DTTM 	= CDF.UPDATED_DTTM,
	TD.UPDATED_DTTM 				= CURRENT_TIMESTAMP,
	TD.UPDATED_BY 					= CURRENT_USER,
	TD.BATCH_ID 					= ` + BATCH_ID + `,
	TD.LAST_DML_CD					= 'U'
FROM
	DAAS_TEMP.COMP_DETAIL_FACT_TRIP CDF
WHERE
	TD.TRANSACTION_TABLE_SK = CDF.COMP_DETAIL_FACT_SK
	AND TD.TRANSACTION_TABLE = 'COMP_DETAIL_FACT'
	AND TD.DELETE_IND = 'N' /* Added this filter to handle if any existing txn got update and this is getting marked as DELETE_IND = N and TRANSACTION_SUB_TYPE as VOIDED */
`;

/*
	Update Modified Date in trip detail record if there is mapping record in COMP_FREE_PLAY_DETAIL_FACT_COMP_DOMAIN_STREAM
*/

my_sql_command_3 =`
UPDATE DAAS_CORE.TRIP_DETAIL TD
SET
	TD.UPDATED_DTTM = CURRENT_TIMESTAMP,
	TD.UPDATED_BY 	= CURRENT_USER,
	TD.BATCH_ID 	= ` + BATCH_ID + `,
	TD.LAST_DML_CD	= 'U'
FROM
	DAAS_CORE.COMP_FREE_PLAY_DETAIL_FACT_COMP_DOMAIN_STREAM CFP
JOIN
	DAAS_CORE.COMP_DETAIL_FACT CDF
ON
	CFP.GUEST_ID 					= CDF.GUEST_ID
	AND CFP.PROPERTY_CD 			= CDF.PROPERTY_CD
	AND DATE(CFP.TRANSACTION_DTTM) 	= DATE(CDF.REDEEM_DTTM)
	AND CDF.ISSUED_BY_CD 			= 'DWNCRD'
	AND CFP.DELETE_IND 				= 'N'
WHERE
	TD.TRANSACTION_TABLE_SK 	= CDF.COMP_DETAIL_FACT_SK
	AND TD.TRANSACTION_TABLE 	= 'COMP_DETAIL_FACT'
`;


/*
	Identify if any other active trip detail records are present for the same transaction_start_dt and inserting to trip_recalc_queue
*/				
my_sql_command_4 =`
INSERT INTO DAAS_CORE.TRIP_RECALC_QUEUE 
( 
	TRIP_MASTER_ID,
	STATUS,
	BATCH_ID,
	CREATED_DTTM,
	CREATED_BY,
	UPDATED_DTTM,
	UPDATED_BY 
)
SELECT 
	TD.TRIP_MASTER_ID,
	'PENDING' AS STATUS,
	` + BATCH_ID + ` ,
	CURRENT_TIMESTAMP AS CREATED_DTTM,
	CURRENT_USER AS CREATED_BY,
	CURRENT_TIMESTAMP AS UPDATED_DTTM,
	CURRENT_USER AS UPDATED_BY
FROM
	DAAS_CORE.TRIP_DETAIL TD
JOIN
	DAAS_TEMP.COMP_DETAIL_FACT_TRIP CDF
ON
	TD.TRANSACTION_TABLE_SK = CDF.COMP_DETAIL_FACT_SK
WHERE
	TD.TRANSACTION_TABLE 	= 'COMP_DETAIL_FACT'
	AND TD.DELETE_IND 		= 'Y'
    AND (CDF.DELETE_IND     = 'Y' OR TD.TRANSACTION_SUB_TYPE = 'TRANSACTION CHANGED')
	AND NOT EXISTS
	(
		SELECT
			1
		FROM
			DAAS_CORE.TRIP_DETAIL TD1
		WHERE
			TD.TRANSACTION_START_DTTM 	= TD1.TRANSACTION_START_DTTM
			AND TD.TRIP_MASTER_ID 		= TD1.TRIP_MASTER_ID
			AND TD1.DELETE_IND 			= 'N'
	)		
`;	

/*
	Populate 2 more Trip Types (Market and Enterprise) for all active records 
*/	
					
my_sql_command_5 =`
INSERT INTO DAAS_TEMP.TRIP_RESULT_COMP_ALL_TRIP_TYPES
SELECT DISTINCT
	CDF.GUEST_UNIQUE_ID,
	IFF(COLUMN1 = 'PROPERTY', CDF.PROPERTY_CD, 'N/A') AS PROPERTY_CD,
	PME.COLUMN1 AS TRIP_TYPE,
	'N/A' AS MARKET_CD,
	CDF.COMP_DETAIL_FACT_SK AS TRANSACTION_TABLE_SK,
	'COMP_DETAIL_FACT' AS TRANSACTION_TABLE,
	'COMP' AS TRANSACTION_TYPE,
	CDF.TRANSACTION_DTTM AS TRANSACTION_START_DTTM,
	CDF.TRANSACTION_DTTM AS TRANSACTION_END_DTTM,
	/*DAAS_COMMON.TRIP_ADJUSTMENT_DATE_UDF(CDF.TRANSACTION_DTTM , DAAS_COMMON.TRIP_CONFIG_UDF_PROPERTY('REPORTING_DATE_CUTOFF', CDF.PROPERTY_CD, CDF.TRANSACTION_DTTM)) AS BUSINESS_START_DT,*/
	CDF.TRANSACTION_DTTM AS BUSINESS_START_DT,
	BUSINESS_START_DT AS BUSINESS_END_DT,
	CDF.UPDATED_DTTM AS TRANSACTION_MODIFIED_DTTM
FROM
	DAAS_TEMP.COMP_DETAIL_FACT_TRIP CDF
JOIN 
(
	SELECT 
		*
	FROM 
	(
		VALUES
			('PROPERTY'),
			('ENTERPRISE')
	)
) PME
WHERE
    CDF.DELETE_IND = 'N'
UNION ALL
SELECT
	CDF.GUEST_UNIQUE_ID,
	'N/A' AS PROPERTY_CD,
	'MARKET' AS TRIP_TYPE,
	PML.MARKET_CD AS MARKET_CD,
	CDF.COMP_DETAIL_FACT_SK AS TRANSACTION_TABLE_SK,
	'COMP_DETAIL_FACT' AS TRANSACTION_TABLE,
	'COMP' AS TRANSACTION_TYPE,
	CDF.TRANSACTION_DTTM AS TRANSACTION_START_DTTM,
	CDF.TRANSACTION_DTTM AS TRANSACTION_END_DTTM,
	/*DAAS_COMMON.TRIP_ADJUSTMENT_DATE_UDF(CDF.TRANSACTION_DTTM , DAAS_COMMON.TRIP_CONFIG_UDF_PROPERTY('REPORTING_DATE_CUTOFF', CDF.PROPERTY_CD, CDF.TRANSACTION_DTTM)) AS BUSINESS_START_DT,*/
	CDF.TRANSACTION_DTTM AS BUSINESS_START_DT,
	BUSINESS_START_DT AS BUSINESS_END_DT,
	CDF.UPDATED_DTTM AS TRANSACTION_MODIFIED_DTTM
FROM
	DAAS_TEMP.COMP_DETAIL_FACT_TRIP CDF
LEFT JOIN
    DAAS_COMMON.PROPERTY_MARKET_LKP PML
ON
    CDF.PROPERTY_CD = PML.PROPERTY_CD
WHERE
    CDF.DELETE_IND = 'N'
`;	


/*	Passing above step records to ITERATION 3 */			
		 
my_sql_command_6 = `CALL DAAS_COMMON.TRIP_IDENTIFIER_PROC(` + BATCH_ID + `,'` + SHARD_NAME + `','DAAS_TEMP.TRIP_RESULT_COMP_ALL_TRIP_TYPES','DAAS_TEMP.TRIP_RESULT_COMP','COMP')`;


/*	Based on result from iteration 3, we are updating or inserting to trip tables */			

my_sql_command_7 =`
UPDATE DAAS_CORE.TRIP_MASTER TM
SET
	TM.TRIP_END_DT 	= SPLIT_PART(TR.TRIP_INDICATOR,'|',3),
	TM.UPDATED_DTTM = CURRENT_TIMESTAMP,
	TM.UPDATED_BY 	= CURRENT_USER,
	TM.BATCH_ID 	= ` + BATCH_ID + `,
	TM.LAST_DML_CD	= 'U'
FROM
	DAAS_TEMP.TRIP_RESULT_COMP TR
WHERE
	SPLIT_PART(TR.TRIP_INDICATOR, '|', 1) 	= 'Y'
	AND TM.TRIP_MASTER_ID 					= IFF(LENGTH(SPLIT_PART(TR.TRIP_INDICATOR,'|',2)) = 0, 0, SPLIT_PART(TR.TRIP_INDICATOR,'|',2))
	AND TR.GUEST_UNIQUE_ID 					= TM.GUEST_UNIQUE_ID
	AND TR.PROPERTY_CD 						= TM.PROPERTY_CD
	AND TR.MARKET_CD 						= TM.MARKET_CD
	AND TR.TRIP_TYPE 						= TM.TRIP_TYPE
	AND TM.DELETE_IND 						= 'N'
	AND LENGTH(SPLIT_PART(TR.TRIP_INDICATOR, '|', 3)) > 0
`; 

my_sql_command_8 =`
INSERT INTO DAAS_CORE.TRIP_DETAIL 
( 
	TRIP_DETAIL_ID,
	TRIP_MASTER_ID,
	TRANSACTION_TABLE_SK,
	TRANSACTION_TABLE,
	TRANSACTION_TYPE,
	TRANSACTION_SUB_TYPE,
	TRANSACTION_START_DTTM,
	TRANSACTION_END_DTTM,
	BUSINESS_START_DT,
	BUSINESS_END_DT,
	TRANSACTION_MODIFIED_DTTM,
	DELETE_IND,
	CREATED_DTTM,
	CREATED_BY,
	UPDATED_DTTM,
	UPDATED_BY,
	BATCH_ID,
	LAST_DML_CD
)
SELECT
	DAAS_CORE.TRIP_DETAIL_SEQ.NEXTVAL TRIP_DETAIL_ID,
	TM.TRIP_MASTER_ID TRIP_MASTER_ID,
	TR.TRANSACTION_TABLE_SK ,
	TR.TRANSACTION_TABLE,
	'COMP' TRANSACTION_TYPE,
	'OPENED' TRANSACTION_SUB_TYPE,
	TR.TRANSACTION_START_DTTM,
	TR.TRANSACTION_END_DTTM,
	TR.BUSINESS_START_DT,
	TR.BUSINESS_END_DT,
	TR.TRANSACTION_MODIFIED_DTTM,
	'N' DELETE_IND,
	CURRENT_TIMESTAMP CREATED_DTTM,
	CURRENT_USER CREATED_BY,
	CURRENT_TIMESTAMP UPDATED_DTTM,
	CURRENT_USER UPDATED_BY,
	` + BATCH_ID + ` BATCH_ID,
	'I'
FROM
	DAAS_TEMP.TRIP_RESULT_COMP TR
JOIN 
	DAAS_CORE.TRIP_MASTER TM 
ON
	TM.GUEST_UNIQUE_ID 	= TR.GUEST_UNIQUE_ID
	AND TM.TRIP_TYPE 	= TR.TRIP_TYPE
	AND TM.PROPERTY_CD 	= TR.PROPERTY_CD
	AND TM.MARKET_CD 	= TR.MARKET_CD
	AND TR.BUSINESS_START_DT BETWEEN TM.TRIP_START_DT AND TM.TRIP_END_DT
WHERE
	TM.DELETE_IND = 'N'
	AND SPLIT_PART(TR.TRIP_INDICATOR, '|', 1) = 'Y' 
	AND NOT EXISTS
	(
		SELECT 
			1
		FROM
			DAAS_CORE.TRIP_DETAIL TD
		WHERE
			TR.TRANSACTION_TYPE 		= TD.TRANSACTION_TYPE
			AND TR.TRANSACTION_TABLE_SK = TD.TRANSACTION_TABLE_SK
			AND TM.TRIP_MASTER_ID 		= TD.TRIP_MASTER_ID
			AND TD.DELETE_IND 			= 'N'
	)
`;		 

/*	Passing unmapped records to Iteration-4 SP */
  
my_sql_command_9 =`CALL DAAS_COMMON.TRIP_FIND_START_DATE_END_DATE_PROC(` + BATCH_ID + `,'` + SHARD_NAME + `','DAAS_TEMP.TRIP_RESULT_COMP','DAAS_TEMP.TRIP_RANGE_COMP')`;	


/*	Based on Iteration-4 SP output, we are inserting new trip to trip tables */	

my_sql_command_10 =`
INSERT INTO DAAS_CORE.TRIP_MASTER 
( 
	TRIP_MASTER_ID,
	GUEST_UNIQUE_ID,
	PROPERTY_CD,
	MARKET_CD,
	TRIP_TYPE,
	TRIP_START_DT,
	TRIP_END_DT,
	DELETE_IND,
	CREATED_DTTM,
	CREATED_BY,
	UPDATED_DTTM,
	UPDATED_BY,
	BATCH_ID,
	LAST_DML_CD 
)
SELECT
	DAAS_CORE.TRIP_MASTER_SEQ.NEXTVAL AS TRIP_MASTER_ID,
	GUEST_UNIQUE_ID,
	PROPERTY_CD,
	MARKET_CD,
	TRIP_TYPE TRIP_TYPE,
	TRIP_START_DT,
	TRIP_END_DT,
	'N' DELETE_IND,
	CURRENT_TIMESTAMP CREATED_DTTM,
	CURRENT_USER CREATED_BY,
	CURRENT_TIMESTAMP UPDATED_DTTM,
	CURRENT_USER UPDATED_BY,
	` + BATCH_ID + ` BATCH_ID,
	'I'
FROM
(
	SELECT DISTINCT
		GUEST_UNIQUE_ID,
		PROPERTY_CD,
		MARKET_CD,
		TRIP_TYPE TRIP_TYPE,
		TRIP_START_DT,
		TRIP_END_DT
	FROM
		DAAS_TEMP.TRIP_RANGE_COMP
)
`; 

my_sql_command_11 =`
INSERT INTO DAAS_CORE.TRIP_DETAIL 
( 
	TRIP_DETAIL_ID,
	TRIP_MASTER_ID,
	TRANSACTION_TABLE_SK,
	TRANSACTION_TABLE,
	TRANSACTION_TYPE,
	TRANSACTION_SUB_TYPE,
	TRANSACTION_START_DTTM,
	TRANSACTION_END_DTTM,
	BUSINESS_START_DT,
	BUSINESS_END_DT,
	TRANSACTION_MODIFIED_DTTM,
	DELETE_IND,
	CREATED_DTTM,
	CREATED_BY,
	UPDATED_DTTM,
	UPDATED_BY,
	BATCH_ID,
	LAST_DML_CD 
)
SELECT
	DAAS_CORE.TRIP_DETAIL_SEQ.NEXTVAL AS TRIP_DETAIL_ID,
	TM.TRIP_MASTER_ID TRIP_MASTER_ID,
	TR.TRANSACTION_TABLE_SK,
	TR.TRANSACTION_TABLE,
	'COMP' TRANSACTION_TYPE,
	'OPENED' TRANSACTION_SUB_TYPE,
	TR.TRANSACTION_START_DTTM,
	TR.TRANSACTION_END_DTTM,
	TR.BUSINESS_START_DT,
	TR.BUSINESS_END_DT,
	TR.TRANSACTION_MODIFIED_DTTM,
	'N' DELETE_IND,
	CURRENT_TIMESTAMP CREATED_DTTM,
	CURRENT_USER CREATED_BY,
	CURRENT_TIMESTAMP UPDATED_DTTM,
	CURRENT_USER UPDATED_BY,
	` + BATCH_ID + ` BATCH_ID,
	'I'
FROM
	DAAS_TEMP.TRIP_RANGE_COMP TR
JOIN 
	DAAS_CORE.TRIP_MASTER TM 
ON
	TM.GUEST_UNIQUE_ID 	= TR.GUEST_UNIQUE_ID
	AND TM.TRIP_TYPE 	= TR.TRIP_TYPE
	AND TM.PROPERTY_CD 	= TR.PROPERTY_CD
	AND TM.MARKET_CD 	= TR.MARKET_CD
	AND TR.BUSINESS_START_DT BETWEEN TM.TRIP_START_DT AND TM.TRIP_END_DT
WHERE
	TM.DELETE_IND = 'N'
`;		

my_sql_command_12 =`SELECT COUNT(*) AS SOURCE_COUNT FROM DAAS_TEMP.COMP_DETAIL_FACT_TRIP`;
my_sql_command_13 =`SELECT NVL(SUM(CASE WHEN LAST_DML_CD = 'U' THEN 1 ELSE 0 END), 0) AS UPDATE_COUNT,  NVL(SUM(CASE WHEN LAST_DML_CD = 'I' THEN 1 ELSE 0 END), 0) AS INSERT_COUNT FROM DAAS_CORE.TRIP_MASTER WHERE BATCH_ID = ` + BATCH_ID;
my_sql_command_14 =`SELECT NVL(SUM(CASE WHEN LAST_DML_CD = 'U' THEN 1 ELSE 0 END), 0) AS UPDATE_COUNT,  NVL(SUM(CASE WHEN LAST_DML_CD = 'I' THEN 1 ELSE 0 END), 0) AS INSERT_COUNT FROM DAAS_CORE.TRIP_DETAIL WHERE BATCH_ID = ` + BATCH_ID;


try
{
	proc_step = "Data_Process";
	snowflake.execute( {sqlText: "BEGIN;" } );
	
	statement1 = snowflake.createStatement( {sqlText: my_sql_command_1 } );
	proc_output = my_sql_command_1;
	statement1.execute();
	
	statement1_1 = snowflake.createStatement( {sqlText: my_sql_command_1_1 } );
	proc_output = my_sql_command_1_1;
	statement1_1.execute();
	
	statement1_2 = snowflake.createStatement( {sqlText: my_sql_command_1_2 } );					
	proc_output = my_sql_command_1_2;
	statement1_2.execute(); 	
	
	statement3 = snowflake.createStatement( {sqlText: my_sql_command_2 } );
	proc_output = my_sql_command_2;
	statement3.execute();
	
	statement3 = snowflake.createStatement( {sqlText: my_sql_command_3 } );
	proc_output = my_sql_command_3;
	statement3.execute();
	
	statement4 = snowflake.createStatement( {sqlText: my_sql_command_4 } );
	proc_output = my_sql_command_4;
	statement4.execute();
	
	statement5 = snowflake.createStatement( {sqlText: my_sql_command_5 } );
	proc_output = my_sql_command_5;
	statement5.execute();
	
	statement6 = snowflake.createStatement( {sqlText: my_sql_command_6 } );
	proc_output = my_sql_command_6;
	statment_trip_result = statement6.execute();
	statment_trip_result.next(); 
				
	if( statment_trip_result.getColumnValue(1) == "FAILURE" ) throw "FAILURE_FROM_TRIP_FUNCTION_TRIP_IDENTIFIER_PROC";	
	
	statement7 = snowflake.createStatement( {sqlText: my_sql_command_7 } );
	proc_output = my_sql_command_7;
	statement7.execute();
	
	statement8 = snowflake.createStatement( {sqlText: my_sql_command_8 } );
	proc_output = my_sql_command_8;
	statement8.execute();
	
	statement9 = snowflake.createStatement( {sqlText: my_sql_command_9 } );
	proc_output = my_sql_command_9;
	statment_trip_result = statement9.execute();
	statment_trip_result.next(); 
				
	if( statment_trip_result.getColumnValue(1) == "FAILURE" ) throw "FAILURE_FROM_TRIP_FUNCTION_TRIP_FIND_START_DATE_END_DATE_PROC";

	statement10 = snowflake.createStatement( {sqlText: my_sql_command_10 } );
	proc_output = my_sql_command_10;
	statement10.execute();
	
	statement11 = snowflake.createStatement( {sqlText: my_sql_command_11 } );
	proc_output = my_sql_command_11;
	statement11.execute();
	
	/*Identify source count and Core Record_Count based on batch_id, LAST_DML_CD*/
	
	proc_output = my_sql_command_12;
	statement12 = snowflake.execute({sqlText: my_sql_command_12 } );
	statement12.next();
	src_rec_count = statement12.getColumnValue(1);
	
	proc_output = my_sql_command_13;
	statement13 = snowflake.execute({sqlText: my_sql_command_13 } );
	statement13.next();
	master_update_count = statement13.getColumnValue(1);
	master_insert_count = statement13.getColumnValue(2);
	
	proc_output = my_sql_command_14;
	statement14 = snowflake.execute({sqlText: my_sql_command_14 } );
	statement14.next();
	detail_update_count = statement14.getColumnValue(1);
	detail_insert_count = statement14.getColumnValue(2);
	
	snowflake.execute( {sqlText: "COMMIT;" } );
	
	
	proc_step = "Update_Metrics";
	
	my_sql_command_15 = "CALL DAAS_COMMON.BATCH_CONTROL_UPDATE_BATCH_METRICS_PROC(" + BATCH_ID + ", '" + SHARD_NAME + "', 'source_record_count', '" + src_rec_count + "')";
	statement15 = snowflake.execute( {sqlText: my_sql_command_15 });
	statement15.next();
	src_rec_count_update_metric_status = statement15.getColumnValue(1);
	
	my_sql_command_16 = "CALL DAAS_COMMON.BATCH_CONTROL_UPDATE_BATCH_METRICS_PROC(" + BATCH_ID + ", '" + SHARD_NAME + "', 'master_update_count', '" + master_update_count + "')";
	statement16 = snowflake.execute( {sqlText: my_sql_command_16 });
	statement16.next();
	master_update_count_update_metric_status = statement16.getColumnValue(1);
	
	my_sql_command_17 = "CALL DAAS_COMMON.BATCH_CONTROL_UPDATE_BATCH_METRICS_PROC(" + BATCH_ID + ", '" + SHARD_NAME + "', 'master_insert_count', '" + master_insert_count + "')";
	statement17 = snowflake.execute( {sqlText: my_sql_command_17 });
	statement17.next();
	master_insert_count_update_metric_status = statement17.getColumnValue(1);	
	
	my_sql_command_18 = "CALL DAAS_COMMON.BATCH_CONTROL_UPDATE_BATCH_METRICS_PROC(" + BATCH_ID + ", '" + SHARD_NAME + "', 'detail_update_count', '" + detail_update_count + "')";
	statement18 = snowflake.execute( {sqlText: my_sql_command_18 });
	statement18.next();
	detail_update_count_update_metric_status = statement18.getColumnValue(1);

	my_sql_command_19 = "CALL DAAS_COMMON.BATCH_CONTROL_UPDATE_BATCH_METRICS_PROC(" + BATCH_ID + ", '" + SHARD_NAME + "', 'detail_insert_count', '" + detail_insert_count + "')";
	statement19 = snowflake.execute( {sqlText: my_sql_command_19 });
	statement19.next();
	detail_insert_count_update_metric_status = statement19.getColumnValue(1);
	
	if ( 
		src_rec_count_update_metric_status.includes("SUCCESS") != true || 
		master_update_count_update_metric_status.includes("SUCCESS") != true || 
		master_insert_count_update_metric_status.includes("SUCCESS") != true || 
		detail_update_count_update_metric_status.includes("SUCCESS") != true ||
		detail_insert_count_update_metric_status.includes("SUCCESS") != true
		)
	{
		proc_output = "SOURCE COUNT METRIC STATUS: " + src_rec_count_update_metric_status + "\nMASTER INSERT COUNT METRIC STATUS: " + master_insert_count_update_metric_status + "\nMASTER UPDATE COUNT METRIC STATUS: " + master_update_count_update_metric_status + "\nDETAIL INSERT COUNT METRIC STATUS: " + detail_insert_count_update_metric_status + "\nDETAIL UPDATE COUNT METRIC STATUS: " + detail_update_count_update_metric_status + "FAILURE RETURNED FROM METRICS";
	}
	
	proc_output = "SUCCESS";
}

catch (err) 
{ 
	snowflake.execute( {sqlText: "ROLLBACK;" } );
	
	proc_output = "FAILURE";
	error_code = "Failed: Code: " + err.code + "  State: " + err.state;
	error_message = "\n  Message: " + err.message + "\nStack Trace:\n" + err.stackTraceTxt;
	error_message = error_message.replace(/["']/g, "");
	if ( proc_step == "Data_Process")
	{
		/*CALL BATCH_CONTROL_UPDATE_BATCH_ERROR_LOG_PROC*/
		snowflake.execute( {sqlText: "ROLLBACK;" } );
		my_sql_command_20 = "CALL DAAS_COMMON.BATCH_CONTROL_UPDATE_BATCH_ERROR_LOG_PROC ('" + BATCH_ID + "','" + SHARD_NAME + "','" + error_code + "','" + error_message + "','','','FATAL','" + tag + "_" + proc_step +"')"	
	}
	else if ( proc_step == "Update_Metrics")
	{
		my_sql_command_20 = "CALL DAAS_COMMON.BATCH_CONTROL_UPDATE_BATCH_ERROR_LOG_PROC ('" + BATCH_ID + "','" + SHARD_NAME + "','" + error_code + "','" + error_message + "','','','INFORMATIONAL','" + tag + "_" + proc_step +"')"
	} 
	snowflake.execute( {sqlText: my_sql_command_20});
}
return proc_output ;
$$ ;