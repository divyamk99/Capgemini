CREATE OR REPLACE PROCEDURE DAAS_EXACT_TARGET_RAW.ET_CLICKS_EVENT_RAW_LOAD_PROC(BATCH_ID FLOAT, SHARD_NAME VARCHAR, WAREHOUSE_NAME VARCHAR, CUSTOM_PARAM1 VARCHAR, CUSTOM_PARAM2 VARCHAR) 
RETURNS VARCHAR
LANGUAGE JAVASCRIPT 
EXECUTE AS CALLER AS 
$$
 
/*
#####################################################################################
Author: Surya Jangareddi
Purpose: Load data from DAAS_EXACT_TARGET_STG.ET_CLICKS_EVENT_STG to DAAS_EXACT_TARGET_RAW.ET_CLICKS_EVENT_RAW table
Input Parameters: BATCH_ID, SHARD_NAME, WAREHOUSE_NAME, CUSTOM_PARAM1, CUSTOM_PARAM2
Output Value: SUCCESS for successful execution and for failed execution it returns sql statement causing issue with error description
Created Date : 02/12/2024
Version: 1.0
#####################################################################################
*/

proc_output = "";
proc_step = "";
snowflake.execute( {sqlText: "USE WAREHOUSE " + WAREHOUSE_NAME} );
tag = BATCH_ID + "_ET_CLICKS_EVENT_RAW_LOAD_PROC";
snowflake.execute( {sqlText: "ALTER SESSION SET QUERY_TAG = '" + tag + "'" });

my_sql_command_1 = `
CREATE OR REPLACE TABLE DAAS_TEMP.ET_CLICKS_EVENT_RAW_TEMP AS
SELECT 
	STG.i_clientid,
	STG.i_sendid,
	STG.c_subscriber_key,
	CASE WHEN TRIM(SRC.C_SUBSCRIBER_KEY) = 11 THEN 
    (CASE WHEN UPPER(TRIM(SRC.C_SUBSCRIBER_KEY)) <> LOWER(TRIM(SRC.C_SUBSCRIBER_KEY)) THEN NULL
     ELSE CAST(SRC.C_SUBSCRIBER_KEY AS DECIMAL(11,0)) END) ELSE NULL END as I_DMID
	STG.c_emailaddress,
	STG.i_subscriberid,
	STG.i_listid,
	STG.d_eventdate,
	STG.c_eventtype,
	STG.i_sendurlid,
	STG.i_urlid,
	STG.c_url,
	STG.c_alias,
	STG.c_batchid,
	STG.c_triggeredsendexternalkey,
	STG.SOURCE_SYSTEM_NAME,
	STG.TIME_ZONE,
	CURRENT_TIMESTAMP AS CREATED_DTTM,
	CURRENT_USER AS CREATED_BY,
	CURRENT_TIMESTAMP AS UPDATED_DTTM,
	CURRENT_USER AS UPDATED_BY,
	` + BATCH_ID + ` AS BATCH_ID,
	STG.REPLAY_COUNTER,
	STG.SOURCE_FILE_NAME,
	'N' AS DELETE_IND,
	'I' AS LAST_DML_CD,
	MD5(
		COALESCE(STG.i_clientid, '')||'~'||
		COALESCE(STG.c_emailaddress::VARCHAR, '')||'~'||
		COALESCE(STG.i_subscriberid::VARCHAR, '')||'~'||
		COALESCE(STG.i_listid::VARCHAR, '')||'~'||
		COALESCE(STG.d_eventdate::VARCHAR, '')||'~'||
		COALESCE(STG.c_eventtype::VARCHAR, '')||'~'||
		COALESCE(STG.i_sendurlid::VARCHAR, '')||'~'||
		COALESCE(STG.i_urlid::VARCHAR, '')||'~'||
		COALESCE(STG.c_url::VARCHAR, '')||'~'||
		COALESCE(STG.c_alias::VARCHAR, '')||'~'||
		COALESCE(STG.c_triggeredsendexternalkey::VARCHAR, '')
	) AS MD5_CHECKSUM
FROM
	DAAS_EXACT_TARGET_STG.ET_CLICKS_EVENT_STG_STREAM STG 
WHERE
	STG.METADATA$ACTION = 'INSERT'
`;   

my_sql_command_2 =`
INSERT INTO DAAS_EXACT_TARGET_RAW.ET_CLICKS_EVENT_RAW
(
	i_clientid,
	i_sendid,
	c_subscriber_key,
	i_dmid,
	c_emailaddress,
	i_subscriberid,
	i_listid,
	d_eventdate,
	c_eventtype,
	i_sendurlid,
	i_urlid,
	c_url,
	c_alias,
	c_batchid,
	c_triggeredsendexternalkey,
	d_timestamp,
	c_quality_cd,
	SOURCE_SYSTEM_NAME,
	TIME_ZONE,
	CREATED_DATE,
	CREATED_BY,
	UPDATED_DATE,
	UPDATED_BY,
	BATCH_ID,
	REPLAY_COUNTER,
	SOURCE_FILE_NAME,
	DELETE_IND,
	LAST_DML_CD,
	MD5_CHECKSUM
) 
SELECT
	SRC.i_clientid,
	SRC.i_sendid,
	SRC.c_subscriber_key,
	SRC.i_dmid,
	SRC.c_emailaddress,
	SRC.i_subscriberid,
	SRC.i_listid,
	SRC.d_eventdate,
	SRC.c_eventtype,
	SRC.i_sendurlid,
	SRC.i_urlid,
	SRC.c_url,
	SRC.c_alias,
	SRC.c_batchid,
	SRC.c_triggeredsendexternalkey,
	CURRENT_TIMESTAMP AS d_timestamp,
	' ' AS c_quality_cd,
	SRC.SOURCE_SYSTEM_NAME,
	SRC.TIME_ZONE,
	SRC.CREATED_DTTM,
	SRC.CREATED_BY,
	SRC.UPDATED_DTTM,
	SRC.UPDATED_BY,
	SRC.BATCH_ID,
	SRC.REPLAY_COUNTER,
	SRC.SOURCE_FILE_NAME,
	SRC.DELETE_IND,
	SRC.LAST_DML_CD
	SRC.MD5_CHECKSUM
FROM	
	DAAS_TEMP.ET_CLICKS_EVENT_RAW_TEMP SRC
WHERE NOT EXISTS
(
	SELECT 
		1
	FROM
		DAAS_EXACT_TARGET_RAW.ET_CLICKS_EVENT_RAW RAW
	WHERE
		SRC.i_dmid = RAW.i_dmid
	AND SRC.i_sendid = RAW.i_sendid
	AND SRC.c_batchid = RAW.c_batchid
)
OR SRC.MD5_CHECKSUM NOT IN
SELECT RAW.MD5_CHECKSUM 
FROM
	DAAS_EXACT_TARGET_RAW.ET_CLICKS_EVENT_RAW RAW
WHERE
		SRC.i_dmid = RAW.i_dmid
	AND SRC.i_sendid = RAW.i_sendid
	AND SRC.c_batchid = RAW.c_batchid
`;

my_sql_command_3 = `SELECT COUNT(*) AS raw_insert_count FROM DAAS_EXACT_TARGET_RAW.ET_CLICKS_EVENT_RAW WHERE BATCH_ID = ` + BATCH_ID;

my_sql_command_4 = `SELECT COUNT(*) AS source_count FROM DAAS_TEMP.ET_CLICKS_EVENT_RAW_TEMP`;	


-- truncate the stage table
	
try
{
	proc_step = "Data_Process";
	snowflake.execute( {sqlText: "BEGIN;" } );

	statement1 = snowflake.createStatement( {sqlText: my_sql_command_1 } );
	proc_output = my_sql_command_1;
	statement1.execute();
	
	statement2 = snowflake.createStatement( {sqlText: my_sql_command_2 } );
	proc_output = my_sql_command_2;
	statement2.execute();
	
	/* Get Insert and update counts */
	
	proc_output = my_sql_command_3;
	statement3 = snowflake.execute( {sqlText: my_sql_command_3 } );
	statement3.next();
	raw_insert_count = statement3.getColumnValue(1);
	
	proc_output = my_sql_command_4;
	statement4 = snowflake.execute( {sqlText: my_sql_command_4 }); 
	statement4.next();
	source_count = statement4.getColumnValue(1);
		
	snowflake.execute( {sqlText: "COMMIT;" } );
	
	proc_step = "Update_Metrics";
	
	my_sql_command_5 = "CALL DAAS_COMMON.BATCH_CONTROL_UPDATE_BATCH_METRICS_PROC(" + BATCH_ID + ", '" + SHARD_NAME + "', 'raw_insert_count', '" + raw_insert_count + "')";
	statement5 = snowflake.execute( {sqlText: my_sql_command_5 });
	statement5.next();
	raw_insert_count_update_metric_status = statement5.getColumnValue(1);
	
	my_sql_command_6 = "CALL DAAS_COMMON.BATCH_CONTROL_UPDATE_BATCH_METRICS_PROC(" + BATCH_ID + ", '" + SHARD_NAME + "', 'source_count', '" + source_count + "')";
	statement6 = snowflake.execute( {sqlText: my_sql_command_6 });
	statement6.next();
	source_count_update_metric_status = statement6.getColumnValue(1);
	
	if ( 
		raw_insert_count_update_metric_status.includes("SUCCESS") != true || 
		source_count_update_metric_status.includes("SUCCESS") != true 
	   )
	{
		proc_output = "RAW INSERT COUNT METRIC STATUS: " + raw_insert_count_update_metric_status + "\nSOURCE COUNT METRIC STATUS: " + source_count_update_metric_status +"FAILURE RETURNED FROM METRICS";
	}
	
	proc_output = "SUCCESS";
} 
catch (err) 
{ 
	proc_output = "FAILURE";
	error_code = "Failed: Code: " + err.code + "  State: " + err.state;
	error_message = "\n  Message: " + err.message + "\nStack Trace:\n" + err.stackTraceTxt;
	error_message = error_message.replace(/["']/g, "");
	if ( proc_step == "Data_Process")
	{
		/*CALL BATCH_CONTROL_UPDATE_BATCH_ERROR_LOG_PROC*/
		snowflake.execute( {sqlText: "ROLLBACK;" } );
		my_sql_command_6 = "CALL DAAS_COMMON.BATCH_CONTROL_UPDATE_BATCH_ERROR_LOG_PROC ('" + BATCH_ID + "','" + SHARD_NAME + "','" + error_code + "','" + error_message + "','','','FATAL','" + tag + "_" + proc_step +"')"	
	}
	else if ( proc_step == "Update_Metrics")
	{
		my_sql_command_6 = "CALL DAAS_COMMON.BATCH_CONTROL_UPDATE_BATCH_ERROR_LOG_PROC ('" + BATCH_ID + "','" + SHARD_NAME + "','" + error_code + "','" + error_message + "','','','INFORMATIONAL','" + tag + "_" + proc_step +"')"
	} 
	snowflake.execute( {sqlText: my_sql_command_6});
}
return proc_output ;
$$ ;