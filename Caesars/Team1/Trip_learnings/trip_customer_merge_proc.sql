CREATE OR REPLACE PROCEDURE DAAS_COMMON.TRIP_CUSTOMER_MERGE_PROC(BATCH_ID FLOAT, SHARD_NAME VARCHAR, WAREHOUSE_NAME VARCHAR, CUSTOM_PARAM1 VARCHAR, CUSTOM_PARAM2 VARCHAR) 
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER AS
$$
/*
#####################################################################################
Author: HEMANTH V 
Purpose:  
Input Parameters: BATCH_ID, SHARD_NAME, WAREHOUSE_NAME, CUSTOM_PARAM1, CUSTOM_PARAM2
Output Value: SUCCESS for successful execution and for failed execution it returns sql statement causing issue with error description
Create Date:  
Version: 1.0
#####################################################################################
*/
proc_output = "";
proc_step = "";
snowflake.execute( {sqlText: "USE WAREHOUSE " + WAREHOUSE_NAME} );
tag = BATCH_ID + "_CUSTOMER_MERGE_PROC";
snowflake.execute( {sqlText: "ALTER SESSION SET QUERY_TAG = '" + tag + "'" });

snowflake.execute( {sqlText: "CREATE OR REPLACE TABLE DAAS_TEMP.TRIP_CUSTOMER_MERGE_QUEUE_TMP AS SELECT *,SPACE(16)::VARCHAR AS ULTIMATE_SURVIVOR_ID FROM DAAS_CORE.TRIP_CUSTOMER_MERGE_QUEUE LIMIT 0;"});

snowflake.execute( {sqlText: "TRUNCATE TABLE DAAS_TEMP.TRIP_CUSTOMER_MERGE_RESULT_TMP;"});

snowflake.execute( {sqlText: "TRUNCATE TABLE DAAS_TEMP.TRIP_CUSTOMER_MERGE_RESULT_TMP_1;"});

snowflake.execute( {sqlText: "TRUNCATE TABLE DAAS_TEMP.CUSTOMER_MERGE_ULTIMATE;"});

try
{
snowflake.execute( {sqlText: "BEGIN;" } );

proc_step = "Data_Process";

/*1) Loading  into  records from Stream into an Intermediate table to find out ultimate survivor account for each victim account*/

snowflake.execute({sqlText:"INSERT INTO  DAAS_TEMP.CUSTOMER_MERGE_ULTIMATE SELECT VICTIM_CUSTOMER_ID,SURVIVOR_CUSTOMER_ID FROM DAAS_CORE.TRIP_CUSTOMER_MERGE_QUEUE_STREAM  WHERE METADATA$ACTION='INSERT';"});
 
 /*
 
   1)Finding Nth level ultimate survivor for the records from xref table 
   
   Note:- Optimization is possible  in finding out ultimate guest 
             1)	By moving   except quey to View or creating an ultimate guest table 
             2)	Moving this entire CTE logic to UDF we can achieve a row-level result set 
  
 */
 
my_sql_command_1 =`	
INSERT
	INTO
	DAAS_TEMP.TRIP_CUSTOMER_MERGE_QUEUE_TMP (VICTIM_CUSTOMER_ID,
	SURVIVOR_CUSTOMER_ID,
	ULTIMATE_SURVIVOR_ID,
	STATUS,
	BATCH_ID )
	
SELECT
	DISTINCT VICTIM_CUSTOMER_ID VICTIM,
	SURVIVOR_CUSTOMER_ID ,
	SURVIVOR_CUSTOMER_ID ULTIMATESURVIVOR,
	'PENDING' STATUS,
	` + BATCH_ID + `	
	from DAAS_TEMP.CUSTOMER_MERGE_ULTIMATE
	`;
 
statement1 = snowflake.createStatement( {sqlText: my_sql_command_1 } );

proc_output = my_sql_command_1;

statement1.execute(); 
				  
get_Source_Rec_Count = snowflake.execute( {sqlText: "SELECT	COUNT(1) FROM DAAS_TEMP.TRIP_CUSTOMER_MERGE_QUEUE_TMP" } );
		
get_Source_Rec_Count.next();
		
Source_Rec_Count = get_Source_Rec_Count.getColumnValue(1);
			
/*
Pulling all the active detailed  records for the victim and ultimate survivor to result_Tmp 
*/				  
				  
				  
				  
my_sql_command_2 =`				  
INSERT
	INTO
	DAAS_TEMP.TRIP_CUSTOMER_MERGE_RESULT_TMP
SELECT
	DISTINCT ULTIMATE_SURVIVOR_ID GUEST_UNIQUE_ID,
		NVL(A.PROPERTY_CD,'N/A') AS PROPERTY_CD,
		A.TRIP_TYPE,
		NVL(A.MARKET_CD,'N/A') AS MARKET_CD,
		B.TRANSACTION_TABLE_SK ,
		B.TRANSACTION_TABLE,
		B.TRANSACTION_TYPE,
		B.TRANSACTION_START_DTTM,
		B.TRANSACTION_END_DTTM,
		B.BUSINESS_START_DT,
		B.BUSINESS_END_DT ,
		B.TRANSACTION_MODIFIED_DTTM,
		'N' AS TRIP_INDICATOR	
FROM
	DAAS_CORE.TRIP_MASTER A
JOIN DAAS_CORE.TRIP_DETAIL B ON
	A.TRIP_MASTER_ID = B.TRIP_MASTER_ID
JOIN DAAS_TEMP.TRIP_CUSTOMER_MERGE_QUEUE_TMP C ON
	A.GUEST_UNIQUE_ID = C.VICTIM_CUSTOMER_ID
WHERE
	A.DELETE_IND = 'N'
	AND B.TRANSACTION_SUB_TYPE = 'OPENED'
UNION
SELECT
	DISTINCT ULTIMATE_SURVIVOR_ID GUEST_UNIQUE_ID,
		NVL(A.PROPERTY_CD,'N/A') AS PROPERTY_CD,
		A.TRIP_TYPE,
		NVL(A.MARKET_CD,'N/A') AS MARKET_CD,
		B.TRANSACTION_TABLE_SK ,
		B.TRANSACTION_TABLE,
		B.TRANSACTION_TYPE,
		B.TRANSACTION_START_DTTM,
		B.TRANSACTION_END_DTTM,
		B.BUSINESS_START_DT,
		B.BUSINESS_END_DT ,
		B.TRANSACTION_MODIFIED_DTTM,
		'N' AS TRIP_INDICATOR 
FROM
	DAAS_CORE.TRIP_MASTER A
JOIN DAAS_CORE.TRIP_DETAIL B ON
	A.TRIP_MASTER_ID = B.TRIP_MASTER_ID
JOIN DAAS_TEMP.TRIP_CUSTOMER_MERGE_QUEUE_TMP C ON
	A.GUEST_UNIQUE_ID = C.ULTIMATE_SURVIVOR_ID
WHERE
	A.DELETE_IND = 'N'
	AND B.TRANSACTION_SUB_TYPE = 'OPENED'
`;

statement2  = snowflake.createStatement( {sqlText: my_sql_command_2 } );

proc_output = my_sql_command_2;

statement2.execute();


/*
Soft Delete Trip Master entries of Victim AND ULTIMATE SURVIVOR Customer Id 
*/


my_sql_command_3 =`
UPDATE
	DAAS_CORE.TRIP_MASTER TM
SET
	TM.DELETE_IND = 'Y',
	TM.UPDATED_DTTM = CURRENT_TIMESTAMP(),
	TM.UPDATED_BY = CURRENT_USER(),
	TM.BATCH_ID =` + BATCH_ID + `,
	TM.LAST_DML_CD = 'U'
FROM
	DAAS_TEMP.TRIP_CUSTOMER_MERGE_QUEUE_TMP TBL
WHERE
	( TM.GUEST_UNIQUE_ID = TBL.VICTIM_CUSTOMER_ID
		OR TM.GUEST_UNIQUE_ID = TBL.ULTIMATE_SURVIVOR_ID)
	AND TM.DELETE_IND = 'N';
 `;

		 
statement3 = snowflake.createStatement( {sqlText: my_sql_command_3 } );
					
proc_output = my_sql_command_3;
					
statement3.execute();
/*
ITERATION 4 Processing (Finding Trip Start Date and End Date in Bulk) 
*/			

my_sql_command_10 =`CALL DAAS_COMMON.TRIP_FIND_START_DATE_END_DATE_PROC(` + BATCH_ID + `,'` + SHARD_NAME + `','DAAS_TEMP.TRIP_CUSTOMER_MERGE_RESULT_TMP','DAAS_TEMP.TRIP_CUSTOMER_MERGE_RESULT_TMP_1');`; 

statement10 = snowflake.createStatement( {sqlText: my_sql_command_10 } );

proc_output = my_sql_command_10;

statment_trip_result=statement10.execute();	
	           
statment_trip_result.next(); 

out=statment_trip_result.getColumnValue(1);
				
				  
if(out == "FAILURE") throw "FAILURE_FROM_TRIP_FUNCTION";					  
			

/*
  Insert in Trip Master and Detail based on the ITERATION 4 out
*/
			
my_sql_command_4 =`INSERT
	INTO
	DAAS_CORE.TRIP_MASTER ( TRIP_MASTER_ID,
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
	BATCH_ID ,
	LAST_DML_CD)
SELECT
	DAAS_CORE.TRIP_MASTER_SEQ.NEXTVAL TRIP_MASTER_ID,
	GUEST_UNIQUE_ID,
	IFF(TRIP_TYPE = 'PROPERTY',PROPERTY_CD,'N/A') PROPERTY_CD,
	IFF(TRIP_TYPE = 'MARKET',MARKET_CD,'N/A') MARKET_CD,
	TRIP_TYPE ,
	TRIP_START_DT,
	TRIP_END_DT,
	'N',
	CURRENT_TIMESTAMP(),
	CURRENT_USER() CREATED_BY,
	CURRENT_TIMESTAMP() UPDATED_DTTM,
	CURRENT_USER() UPDATED_BY,
	` + BATCH_ID + `,
	'I'
FROM
	(
	SELECT
		DISTINCT TRIP_START_DT,
		TRIP_END_DT,
		TRIP_TYPE,
		PROPERTY_CD,
		MARKET_CD,
		GUEST_UNIQUE_ID
	FROM
		DAAS_TEMP.TRIP_CUSTOMER_MERGE_RESULT_TMP_1 RT
	ORDER BY
		GUEST_UNIQUE_ID ,
		TRIP_START_DT ASC );
	`;
				
statement4 = snowflake.createStatement( {sqlText: my_sql_command_4 } );

proc_output = my_sql_command_4;

statement4.execute();
				
 my_sql_command_5 =`INSERT
	INTO
	DAAS_CORE.TRIP_DETAIL( TRIP_DETAIL_ID,
	TRIP_MASTER_ID ,
	TRANSACTION_TABLE_SK ,
	TRANSACTION_TABLE ,
	TRANSACTION_TYPE ,
	TRANSACTION_SUB_TYPE ,
	TRANSACTION_START_DTTM ,
	TRANSACTION_END_DTTM ,
	BUSINESS_START_DT ,
	BUSINESS_END_DT ,
	TRANSACTION_MODIFIED_DTTM,
	DELETE_IND ,
	CREATED_DTTM ,
	CREATED_BY ,
	UPDATED_DTTM ,
	UPDATED_BY ,
	BATCH_ID,
	LAST_DML_CD)
SELECT
	DAAS_CORE.TRIP_DETAIL_SEQ.NEXTVAL TRIP_DETAIL_ID,
	TRIP_MASTER_ID TRIP_MASTER_ID,
	TRANSACTION_TABLE_SK ,
	TRANSACTION_TABLE,
	TRANSACTION_TYPE,
	'OPENED',
	TRANSACTION_START_DTTM,
	TRANSACTION_END_DTTM,
	BUSINESS_START_DT,
	BUSINESS_END_DT,
	TRANSACTION_MODIFIED_DTTM,
	'N',
	CURRENT_TIMESTAMP() CREATED_DTTM,
	CURRENT_USER() CREATED_BY,
	CURRENT_TIMESTAMP() UPDATED_DTTM,
	CURRENT_USER() UPDATED_BY,
	` + BATCH_ID + `,
	'I'
FROM
	(
	SELECT
		DISTINCT RT.*,
		TRIP_MASTER_ID
	FROM
		DAAS_TEMP.TRIP_CUSTOMER_MERGE_RESULT_TMP_1 RT
	JOIN DAAS_CORE.TRIP_MASTER TM ON
		RT.GUEST_UNIQUE_ID = TM.GUEST_UNIQUE_ID
		AND TM.TRIP_TYPE = RT.TRIP_TYPE
		AND TM.PROPERTY_CD = RT.PROPERTY_CD
		AND TM.MARKET_CD = RT.MARKET_CD
		AND TM.DELETE_IND = 'N'
		AND RT.BUSINESS_START_DT BETWEEN TM.TRIP_START_DT AND TM.TRIP_END_DT)
ORDER BY
	TRIP_DETAIL_ID ASC,
	TRIP_MASTER_ID ASC;
		  `;

statement5 = snowflake.createStatement( {sqlText: my_sql_command_5 } );

proc_output = my_sql_command_5;

statement5.execute();


/*
INSERTING PROCESSED records into  TRIP_CUSTOMER_MERGE_PROCESSED with status as PROCESSED
*/


my_sql_command_6 =`INSERT
	INTO
	DAAS_CORE.TRIP_CUSTOMER_MERGE_PROCESSED(
	VICTIM_CUSTOMER_ID,
	SURVIVOR_CUSTOMER_ID,
	ULTIMATE_SURVIVOR_ID,
	STATUS,
	BATCH_ID, 
	CREATED_DTTM,
	CREATED_BY,
	UPDATED_DTTM,
	UPDATED_BY  
)
SELECT
	VICTIM_CUSTOMER_ID,
	SURVIVOR_CUSTOMER_ID,
	ULTIMATE_SURVIVOR_ID,
	'PROCESSED',
  ` + BATCH_ID + `,
	CURRENT_TIMESTAMP(),
	CURRENT_USER(),
	CURRENT_TIMESTAMP(),
	CURRENT_USER()  
FROM
	DAAS_TEMP.TRIP_CUSTOMER_MERGE_QUEUE_TMP ;`;

statement6 = snowflake.createStatement( {sqlText: my_sql_command_6 } );

proc_output = my_sql_command_6;

statement6.execute();   
					
snowflake.execute( {sqlText: "COMMIT;" } ); 	

/*Identify Core Record_Count based on batch_id, LAST_DML_CD*/
var get_Master_Count = snowflake.execute( {sqlText: "SELECT NVL(SUM(CASE WHEN LAST_DML_CD = 'U' THEN 1 ELSE 0 END), 0) AS UPDATE_COUNT,  NVL(SUM(CASE WHEN LAST_DML_CD = 'I' THEN 1 ELSE 0 END), 0) AS INSERT_COUNT FROM DAAS_CORE.TRIP_MASTER WHERE BATCH_ID = " + BATCH_ID + "" } );

get_Master_Count.next();

Master_Update_Count = get_Master_Count.getColumnValue(1);

Master_Insert_Count = get_Master_Count.getColumnValue(2);

var get_Detail_Count = snowflake.execute( {sqlText: "SELECT NVL(SUM(CASE WHEN LAST_DML_CD = 'U' THEN 1 ELSE 0 END), 0) AS UPDATE_COUNT,  NVL(SUM(CASE WHEN LAST_DML_CD = 'I' THEN 1 ELSE 0 END), 0) AS INSERT_COUNT FROM DAAS_CORE.TRIP_DETAIL WHERE BATCH_ID = " + BATCH_ID + "" } );

get_Detail_Count.next();

Detail_Update_Count = get_Detail_Count.getColumnValue(1);

Detail_Insert_Count = get_Detail_Count.getColumnValue(2);


/*Call Update_Batch_Metrics for each inserting each count metrics*/
var call_source_rec_count = snowflake.execute({sqlText: "CALL DAAS_COMMON.BATCH_CONTROL_UPDATE_BATCH_METRICS_PROC('" + BATCH_ID + "' , '" + SHARD_NAME + "', 'Source_Rec_Count', '" + Source_Rec_Count + "');" });

var call_master_insert_count = snowflake.execute({sqlText:"CALL DAAS_COMMON.BATCH_CONTROL_UPDATE_BATCH_METRICS_PROC('" + BATCH_ID + "' , '" + SHARD_NAME + "', 'Master_Insert_Count' , '" + Master_Insert_Count + "');" });

var call_master_update_count = snowflake.execute({sqlText:"CALL DAAS_COMMON.BATCH_CONTROL_UPDATE_BATCH_METRICS_PROC('" + BATCH_ID + "' , '" + SHARD_NAME + "', 'Master_Update_Count' , '" + Master_Update_Count + "');" });

var call_detail_insert_count = snowflake.execute({sqlText:"CALL DAAS_COMMON.BATCH_CONTROL_UPDATE_BATCH_METRICS_PROC('" + BATCH_ID + "' , '" + SHARD_NAME + "', 'Detail_Insert_Count' , '" + Detail_Insert_Count + "');" });

var call_detail_update_count = snowflake.execute({sqlText:"CALL DAAS_COMMON.BATCH_CONTROL_UPDATE_BATCH_METRICS_PROC('" + BATCH_ID + "' , '" + SHARD_NAME + "', 'Detail_Update_Count' , '" + Detail_Update_Count + "');" });

call_source_rec_count.next();

call_master_insert_count.next();

call_master_update_count.next();

call_detail_insert_count.next();

call_detail_update_count.next();

var get_val_source_count = call_source_rec_count.getColumnValue(1);

var get_val_master_insert_count = call_master_insert_count.getColumnValue(1);

var get_val_master_update_count = call_master_update_count.getColumnValue(1);

var get_val_detail_insert_count = call_detail_insert_count.getColumnValue(1);

var get_val_detail_update_count = call_detail_update_count.getColumnValue(1);

/*Error Handling if Metrics Update Failed*/
if (get_val_source_count.includes("SUCCESS") != true || get_val_master_insert_count.includes("SUCCESS") != true || get_val_master_update_count.includes("SUCCESS") != true || get_val_detail_insert_count.includes("SUCCESS") != true || get_val_detail_update_count.includes("SUCCESS") != true) { proc_output = "SOURCE COUNT METRIC STATUS: " + get_val_source_count + "\nMASTER INSERT COUNT METRIC STATUS: " + get_val_master_insert_count + + "\nMASTER UPDATE COUNT METRIC STATUS: " + get_val_master_update_count +"\nDETAIL INSERT COUNT METRIC STATUS: " + get_val_detail_insert_count + "\nDETAIL UPDATE COUNT METRIC STATUS: " + get_val_detail_update_count +"\nFAILURE RETURNED FROM METRICS";

}
else { proc_output = "SUCCESS";

}/*Commit the Update_Metrics Step */
snowflake.execute( {sqlText: "COMMIT;" } ); 

var my_sql_command_81 = "";

proc_output = "SUCCESS";
		
}				 
		
catch (err) 

{  

if(err.message=="FAILURE_FROM_TRIP_FUNCTION") {proc_output = "FAILURE_FROM_TRIP_FUNCTION";} else {proc_output = "FAILURE";} ;
					  
error_code = "Failed: Code: " + err.code + "  State: " + err.state;

error_message = "\n  Message: " + err.message + "\nStack Trace:\n" + err.stackTraceTxt;

error_message = error_message.replace(/["'"]/g, "");
					
if ( proc_step == "Data_Process")
{
/*CALL BATCH_CONTROL_UPDATE_BATCH_ERROR_LOG_PROC*/
snowflake.execute( {sqlText: "ROLLBACK;" } );
						 
my_sql_command_81 = "CALL DAAS_COMMON.BATCH_CONTROL_UPDATE_BATCH_ERROR_LOG_PROC ('" + BATCH_ID + "','" + SHARD_NAME + "','" + error_code + "','" + error_message + "','','','FATAL','" + tag + "_" + proc_step +"')"	
}
else 
{
snowflake.execute( {sqlText: "ROLLBACK;" } );
						
my_sql_command_81 = "CALL DAAS_COMMON.BATCH_CONTROL_UPDATE_BATCH_ERROR_LOG_PROC ('" + BATCH_ID + "','" + SHARD_NAME + "','" + error_code + "','" + error_message + "','','','INFORMATIONAL','" + tag + "_" + proc_step +"')"
					
} 
					
snowflake.execute( {sqlText: my_sql_command_81});
				
}
				
return proc_output ;

$$ ;