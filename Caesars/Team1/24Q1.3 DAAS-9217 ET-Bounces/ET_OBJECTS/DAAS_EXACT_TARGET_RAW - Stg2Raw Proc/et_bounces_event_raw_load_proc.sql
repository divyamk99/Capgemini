CREATE OR REPLACE PROCEDURE DAAS_EXACT_TARGET_RAW.ET_BOUNCES_EVENT_RAW_LOAD_PROC(BATCH_ID FLOAT, SHARD_NAME VARCHAR, WAREHOUSE_NAME VARCHAR, CUSTOM_PARAM1 VARCHAR, CUSTOM_PARAM2 VARCHAR) 
RETURNS VARCHAR
LANGUAGE JAVASCRIPT 
EXECUTE AS CALLER AS 
$$
 
/*
#####################################################################################
Author: Divya
Purpose: Load data from DAAS_EXACT_TARGET_STG.ET_BOUNCES_EVENT_STG to DAAS_EXACT_TARGET_RAW.ET_BOUNCES_EVENT_RAW table
Input Parameters: BATCH_ID, SHARD_NAME, WAREHOUSE_NAME, CUSTOM_PARAM1, CUSTOM_PARAM2
Output Value: SUCCESS for successful execution and for failed execution it returns sql statement causing issue with error description
Created Date : 02/13/2024
Version: 1.0
#####################################################################################
*/

proc_output = "";
proc_step = "";
snowflake.execute( {sqlText: "USE WAREHOUSE " + WAREHOUSE_NAME} );
tag = BATCH_ID + "_ET_BOUNCES_EVENT_RAW_LOAD_PROC";
snowflake.execute( {sqlText: "ALTER SESSION SET QUERY_TAG = '" + tag + "'" });

my_sql_command_1 = `
CREATE OR REPLACE TABLE DAAS_TEMP.ET_BOUNCES_EVENT_RAW_TEMP AS
SELECT 
	STG.I_CLIENTID AS I_CLIENTID,
	STG.I_SENDID AS I_SENDID,
	STG.C_SUBSCRIBER_KEY AS C_SUBSCRIBER_KEY,
	DAAS_COMMON.EXACT_TARGET_I_DMID_UDF(STG.C_SUBSCRIBER_KEY) AS I_DMID,
	STG.C_EMAILADDRESS AS C_EMAILADDRESS,
	STG.I_SUBSCRIBERID AS I_SUBSCRIBERID,
	STG.I_LISTID AS I_LISTID,
	TO_TIMESTAMP(TO_DATE(SPLIT_PART(STG.D_EVENTDATE, ' ', 1)) ||' '|| SPLIT_PART(STG.D_EVENTDATE, ' ', 2)) AS D_EVENTDATE,
	STG.C_EVENTTYPE AS C_EVENTTYPE,
	STG.C_BOUNCE_CATEGORY AS C_BOUNCE_CATEGORY,
	STG.I_SMTP_CODE AS I_SMTP_CODE,
	STG.C_BOUNCE_REASON AS C_BOUNCE_REASON,
	STG.C_BATCHID AS C_BATCHID,
	STG.C_TRIGGEREDSENDEXTERNALKEY AS C_TRIGGEREDSENDEXTERNALKEY,
	STG.SOURCE_SYSTEM_NAME AS SOURCE_SYSTEM_NAME,
	STG.TIME_ZONE AS TIME_ZONE,
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
		COALESCE(STG.c_bounce_category::VARCHAR, '')||'~'||
		COALESCE(STG.i_smtp_code::VARCHAR, '')||'~'||
		COALESCE(STG.c_bounce_reason::VARCHAR, '')||'~'||
		COALESCE(STG.c_triggeredsendexternalkey::VARCHAR, '')
	) AS MD5_CHECKSUM
FROM
	DAAS_EXACT_TARGET_STG.ET_BOUNCES_EVENT_STG_STREAM STG 
WHERE 
	STG.METADATA$ACTION = 'INSERT'
`;   

my_sql_command_2 =`
INSERT INTO DAAS_EXACT_TARGET_RAW.ET_BOUNCES_EVENT_RAW
(
	I_CLIENTID,                
	I_SENDID,                  
	C_SUBSCRIBER_KEY,          
	I_DMID,                    
	C_EMAILADDRESS,            
	I_SUBSCRIBERID,            
	I_LISTID,                  
	D_EVENTDATE,               
	C_EVENTTYPE,               
	C_BOUNCE_CATEGORY,         
	I_SMTP_CODE,               
	C_BOUNCE_REASON,           
	C_BATCHID,                 
	C_TRIGGEREDSENDEXTERNALKEY,
	C_QUALITY_CD, 			  
	D_TIMESTAMP,               
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
	SRC.I_CLIENTID,                
	SRC.I_SENDID,                  
	SRC.C_SUBSCRIBER_KEY,          
	SRC.I_DMID,                    
	SRC.C_EMAILADDRESS,            
	SRC.I_SUBSCRIBERID,            
	SRC.I_LISTID,                  
	SRC.D_EVENTDATE,               
	SRC.C_EVENTTYPE,               
	SRC.C_BOUNCE_CATEGORY,        
	SRC.I_SMTP_CODE,               
	SRC.C_BOUNCE_REASON,          
	SRC.C_BATCHID,                 
	SRC.C_TRIGGEREDSENDEXTERNALKEY,
	' ' AS C_QUALITY_CD,			  
	CURRENT_TIMESTAMP AS D_TIMESTAMP,               
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
	SRC.LAST_DML_CD,				  
	SRC.MD5_CHECKSUM
FROM	
	DAAS_TEMP.ET_BOUNCES_EVENT_RAW_TEMP SRC
WHERE NOT EXISTS
(
	SELECT 
		1
	FROM
		DAAS_EXACT_TARGET_RAW.ET_BOUNCES_EVENT_RAW RAW
	WHERE
		SRC.I_SENDID = RAW.I_SENDID
	AND SRC.I_DMID = RAW.I_DMID
	AND SRC.C_BATCHID = RAW.C_BATCHID
)
OR SRC.MD5_CHECKSUM NOT IN (
SELECT RAW.MD5_CHECKSUM 
FROM
		DAAS_EXACT_TARGET_RAW.ET_BOUNCES_EVENT_RAW RAW
	WHERE
		SRC.I_SENDID = RAW.I_SENDID
	AND SRC.I_DMID = RAW.I_DMID
	AND SRC.C_BATCHID = RAW.C_BATCHID
)
`;

my_sql_command_3 = `SELECT COUNT(*) AS raw_insert_count FROM DAAS_EXACT_TARGET_RAW.ET_BOUNCES_EVENT_RAW WHERE BATCH_ID = ` +BATCH_ID;

my_sql_command_4 = `SELECT COUNT(*) AS source_count FROM DAAS_TEMP.ET_BOUNCES_EVENT_RAW_TEMP`;	
	
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