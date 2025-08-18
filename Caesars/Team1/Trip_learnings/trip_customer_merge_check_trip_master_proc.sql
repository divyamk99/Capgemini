CREATE OR REPLACE PROCEDURE DAAS_COMMON.TRIP_CUSTOMER_MERGE_CHECK_TRIP_MASTER_PROC(
BATCH_ID FLOAT,
SHARD_NAME VARCHAR,
WAREHOUSE_NAME VARCHAR,
CUSTOM_PARAM1 VARCHAR,
CUSTOM_PARAM2 VARCHAR) 
RETURNS VARCHAR NOT NULL 
LANGUAGE JAVASCRIPT 
EXECUTE AS CALLER AS 
$$  
/*
#####################################################################################
AUTHOR: NAMBI ARASAPPAN
PURPOSE: IDENTIFIES CUSTOMER ID WHICH NEEDS TO BE MERGED FROM TRIP MASTER STREAM.
INPUT PARAMETERS: BATCH_ID, SHARD_NAME, WAREHOUSE_NAME, CUSTOM_PARAM1, CUSTOM_PARAM2
OUTPUT VALUE: SUCCESS FOR SUCCESSFUL EXECUTION ALONG WITH DML INTO TRIP MASTER AND TRIP DETAIL TABLE AND FAILURE FOR FAILED EXECUTION
CREATE DATE: 07/22/2022
VERSION: 1.0 

#########################################################
Modified By:Priyanka V
purpose: Switching the guest_xref_lkp table to guest_xref_bridge_lkp table
modified date:10/01/2023
Version:1.1
#####################################################################################
*/
try
{
proc_output = "";

proc_step = "";

snowflake.execute( {sqlText: "USE WAREHOUSE " + WAREHOUSE_NAME} );

tag = BATCH_ID + "_TRIP_CUSTOMER_MERGE_CHECK_TRIP_MASTER_PROC";

snowflake.execute( {sqlText: "ALTER SESSION SET QUERY_TAG  = '" + tag + "'" });

var get_Source_Rec_Count = snowflake.execute( {sqlText: "SELECT	COUNT(TRIP_MASTER_ID) FROM DAAS_CORE.TRIP_MASTER_TRIP_CUSTOMER_MERGE_CHECK_STREAM WHERE METADATA$ACTION='INSERT'" } );

get_Source_Rec_Count.next();

Source_Rec_Count = get_Source_Rec_Count.getColumnValue(1);

snowflake.execute( {sqlText: "CREATE OR REPLACE TABLE DAAS_TEMP.CUSTOMER_MERGE_CHECK AS SELECT 0::NUMBER(38,0) AS GUEST_UNIQUE_ID" } );

snowflake.execute( {sqlText: "CREATE OR REPLACE TABLE DAAS_TEMP.CUSTOMER_MERGE_CHECK_2 AS SELECT * FROM DAAS_TEMP.CUSTOMER_MERGE_CHECK TMP JOIN DAAS_CORE.GUEST_XREF_BRIDGE_LKP XREF ON TMP.GUEST_UNIQUE_ID = XREF.XREF_ACCOUNT_NBR LIMIT 0;" } );

snowflake.execute( {sqlText: "BEGIN;" } );

proc_step = "Data_Process";

/*Loads distinct guest unique id into a temp table*/
var my_sql_command_1 = `
INSERT INTO	DAAS_TEMP.CUSTOMER_MERGE_CHECK 
SELECT
	DISTINCT TRIP_MASTER_STREAM.GUEST_UNIQUE_ID
FROM
	DAAS_CORE.TRIP_MASTER_TRIP_CUSTOMER_MERGE_CHECK_STREAM TRIP_MASTER_STREAM
JOIN 
	DAAS_CORE.GUEST_DIM DIM
ON 
	DIM.GUEST_UNIQUE_ID = TRIP_MASTER_STREAM.GUEST_UNIQUE_ID
WHERE
	DIM.DELETE_IND = 'Y' 
AND	TRIP_MASTER_STREAM.METADATA$ACTION = 'INSERT' 
AND TRIP_MASTER_STREAM.DELETE_IND = 'N'
ORDER BY
	TRIP_MASTER_STREAM.GUEST_UNIQUE_ID ASC
;`

statement1 = snowflake.createStatement({sqlText: my_sql_command_1});

statement1.execute();

result_set_0 = snowflake.execute( {sqlText: "SELECT COUNT(*) FROM DAAS_TEMP.CUSTOMER_MERGE_CHECK" } );

result_set_0.next();

var get_row_count = result_set_0.getColumnValue(1);

if ( get_row_count != 0 ) 
{

/*For each Guest Unique Id, find out it's survivor id and victim id by joining with XREF BRIDGE LKP table*/
var my_sql_command_2 = `
INSERT INTO DAAS_TEMP.CUSTOMER_MERGE_CHECK_2
(
	GUEST_UNIQUE_ID,
	PRIMARY_ACCOUNT_NBR,
	XREF_ACCOUNT_NBR,
	SOURCE_SYSTEM_NM,
	TIME_ZONE,
	CREATED_DTTM,
	CREATED_BY,
	UPDATED_DTTM,
	UPDATED_BY,
	BATCH_ID,
	MAPPING_SOURCE,
    REPLAY_COUNTER
)	
SELECT
	GUEST_UNIQUE_ID,
	PRIMARY_ACCOUNT_NBR,
	XREF_ACCOUNT_NBR,
	SOURCE_SYSTEM_NM,
	TIME_ZONE,
	CREATED_DTTM,
	CREATED_BY,
	UPDATED_DTTM,
	UPDATED_BY,
	BATCH_ID,
	MAPPING_SOURCE,
    REPLAY_COUNTER		
FROM
	DAAS_TEMP.CUSTOMER_MERGE_CHECK TMP
JOIN 
	DAAS_CORE.GUEST_XREF_BRIDGE_LKP XREF
ON
	TMP.GUEST_UNIQUE_ID = XREF.XREF_ACCOUNT_NBR
WHERE
	 XREF.PRIMARY_ACCOUNT_NBR <> XREF.XREF_ACCOUNT_NBR AND
     XREF.PRIMARY_ACCOUNT_NBR <> '-1'`
     /* QUALIFY ROW_NUMBER() OVER (PARTITION BY XREF.XREF_ACCOUNT_NBR
ORDER BY
	XREF.KEY_SEQUENCE_NBR DESC) = 1;*/
	
statement2 = snowflake.createStatement({sqlText: my_sql_command_2});

statement2.execute();

/*Loads distinct trip merge candidates into the queue table*/
var my_sql_command_3 =`
INSERT INTO DAAS_CORE.TRIP_CUSTOMER_MERGE_QUEUE
(
	VICTIM_CUSTOMER_ID,
	SURVIVOR_CUSTOMER_ID,
	STATUS,
	BATCH_ID,
	CREATED_DTTM,
	CREATED_BY,
	UPDATED_DTTM,
	UPDATED_BY,
	REPLAY_COUNTER
)
SELECT
	DISTINCT 
	TMP.XREF_ACCOUNT_NBR AS VICTIM_CUSTOMER_ID,
	TMP.PRIMARY_ACCOUNT_NBR AS SURVIVOR_CUSTOMER_ID,
	'PENDING',
	` + BATCH_ID + `,
	CURRENT_TIMESTAMP(),
	CURRENT_USER(),
	CURRENT_TIMESTAMP(),
	CURRENT_USER(),
	NULL
FROM 
	DAAS_TEMP.CUSTOMER_MERGE_CHECK_2 TMP
WHERE
	NOT EXISTS 
	(
		SELECT
				1
		FROM
				DAAS_CORE.TRIP_CUSTOMER_MERGE_QUEUE
		WHERE
				TMP.XREF_ACCOUNT_NBR = VICTIM_CUSTOMER_ID
			AND TMP.PRIMARY_ACCOUNT_NBR = SURVIVOR_CUSTOMER_ID
			AND STATUS = 'PENDING'
	)
	AND EXISTS 
	(
		SELECT
			1
		FROM
			DAAS_CORE.TRIP_MASTER TM
		WHERE
			TM.GUEST_UNIQUE_ID = TMP.XREF_ACCOUNT_NBR
			AND TM.DELETE_IND = 'N'
	)
;`
	
statement3 = snowflake.createStatement({sqlText: my_sql_command_3});

statement3.execute();
}
snowflake.execute( {sqlText: "COMMIT;" } );

/*Identify Core Record_Count based on batch_id, LAST_DML_CD*/
var get_merge_queue_insert_count = snowflake.execute( {sqlText: "SELECT COUNT(VICTIM_CUSTOMER_ID) FROM DAAS_CORE.TRIP_CUSTOMER_MERGE_QUEUE WHERE BATCH_ID = " + BATCH_ID + "" } );

get_merge_queue_insert_count.next();

Merge_Queue_Insert_Count = get_merge_queue_insert_count.getColumnValue(1);

/*Call Update_Batch_Metrics for each inserting each count metrics*/
var call_source_rec_count = snowflake.execute({sqlText: "CALL DAAS_COMMON.BATCH_CONTROL_UPDATE_BATCH_METRICS_PROC('" + BATCH_ID + "' , '" + SHARD_NAME + "', 'Source_Rec_Count', '" + Source_Rec_Count + "');" });

var call_merge_queue_insert_count = snowflake.execute({sqlText:"CALL DAAS_COMMON.BATCH_CONTROL_UPDATE_BATCH_METRICS_PROC('" + BATCH_ID + "' , '" + SHARD_NAME + "', 'Merge_Queue_Insert_Count' , '" + Merge_Queue_Insert_Count + "');" });

call_source_rec_count.next();

call_merge_queue_insert_count.next();

var get_val_source_count = call_source_rec_count.getColumnValue(1);

var get_val_merge_queue_insert_count = call_merge_queue_insert_count.getColumnValue(1);

/*Error Handling if Metrics Update Failed*/
if (get_val_source_count.includes("SUCCESS") != true || get_val_merge_queue_insert_count.includes("SUCCESS") != true) { proc_output = "SOURCE COUNT METRIC STATUS: " +  + "\nMERGE QUEUE INSERT COUNT METRIC STATUS: " + get_val_merge_queue_insert_count+ "\nFAILURE RETURNED FROM METRICS";

}
else { proc_output = "SUCCESS";

}
snowflake.execute( {sqlText: "COMMIT;" } );

proc_output = "SUCCESS";

var my_sql_command_31 = "";

}
catch (err) 
{ 
    proc_output = "FAILURE";
    error_code = "Failed: Code: " + err.code + "  State: " + err.state;
    error_message = "\n  Message: " + err.message + "\nStack Trace:\n" + err.stackTraceTxt;
    error_message = error_message.replace(/["'"]/g, "");
    if ( proc_step == "Data_Process")
		{
			/*CALL BATCH_CONTROL_UPDATE_BATCH_ERROR_LOG_PROC*/
			snowflake.execute( {sqlText: "ROLLBACK;" } );
			my_sql_command_31 = "CALL DAAS_COMMON.BATCH_CONTROL_UPDATE_BATCH_ERROR_LOG_PROC ('" + BATCH_ID + "','" + SHARD_NAME + "','" + error_code + "','" + error_message + "','','','FATAL','" + tag + "_" + proc_step +"')"	
		}
		else 
		{ 
			my_sql_command_31 = "CALL DAAS_COMMON.BATCH_CONTROL_UPDATE_BATCH_ERROR_LOG_PROC ('" + BATCH_ID + "','" + SHARD_NAME + "','" + error_code + "','" + error_message + "','','','INFORMATIONAL','" + tag + "_" + proc_step +"')"
		} 
		snowflake.execute( {sqlText: my_sql_command_31});
}
return proc_output ;
$$ ;