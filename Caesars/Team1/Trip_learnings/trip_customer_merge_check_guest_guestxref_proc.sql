CREATE OR REPLACE PROCEDURE DAAS_COMMON.TRIP_CUSTOMER_MERGE_CHECK_GUEST_GUESTXREF_PROC(BATCH_ID FLOAT,SHARD_NAME VARCHAR,WAREHOUSE_NAME VARCHAR,CUSTOM_PARAM1 VARCHAR,CUSTOM_PARAM2 VARCHAR) 
RETURNS VARCHAR 
LANGUAGE JAVASCRIPT 
EXECUTE AS CALLER 
AS 
$$ 
/*
#########################################################################################################################################
Author: HEMANTH V 
Purpose:  
Input Parameters: BATCH_ID, SHARD_NAME, WAREHOUSE_NAME, CUSTOM_PARAM1, CUSTOM_PARAM2
Output Value: SUCCESS for successful execution and for failed execution it returns sql statement causing issue with error description
Create Date:  
Version: 1.0
Modified By: Priyanka V
Purpose: Switching the guest_xref_lkp table to guest_xref_bridge_lkp
Modified date: 10/01/2023
Version: 1.1
Modified By: Surya Jangareddi
Modified Date: 06/28/2023
Version: 1.2 [Removed GUEST_DIM join in DAAS_TEMP.GUEST_XREF_BRIDGE_LKP_TMP to handle if both guests are active in GUEST_DIM]
#########################################################################################################################################
*/

proc_output = "";
proc_step = "";

snowflake.execute( {sqlText: "USE WAREHOUSE " + WAREHOUSE_NAME} );

tag = BATCH_ID + "_TRIP_CUSTOMER_MERGE_CHECK_GUEST_GUESTXREF_PROC";
snowflake.execute( {sqlText: "ALTER SESSION SET QUERY_TAG = '" + tag + "'" });

snowflake.execute( {sqlText: "CREATE OR REPLACE TABLE DAAS_TEMP.GUEST_XREF_BRIDGE_LKP_TMP AS SELECT  * FROM   DAAS_CORE.GUEST_XREF_BRIDGE_LKP  LIMIT 0;" });

/* snowflake.execute( {sqlText: "TRUNCATE TABLE DAAS_CORE.TRIP_CUSTOMER_MERGE_QUEUE;"}); */

try 
{ 
	snowflake.execute( {sqlText: "BEGIN;" } );
	
	proc_step = "Data_Process";

/*
1) USING Guest Xref bridge Stream and Guest Stream 
2) Checking XREF Account Number a PRIMARY Account (Checked based on GUEST_DIM (GUEST_UNIQUE_ID = XREF_ACCOUNT_NBR  and DELETE_IND = 'N')) 
3) ignoring  matched  records as per point 2 
4) processing non-matched records.
5) If entry is present in the Xref bridge table showing this relationship however 
     If it is not yet deleted from guest dim (due to data load issue) then  primary account  will be active in the Xref table  which is wrong,
   to handle such a scenario, we are using  Guest stream.
6)Ids having -1 as primary acnt nbr in xref bridge table are excluded and when valid survivor id comes to -1 in bridge table,then customer merge process should trigger,because if trip is created with -1 as guest id then process doesn’t knows  -1 should be mapped to which primary id's of the victims respectively,as the victims would be already soft deleted and no more victims.							
*/

	my_sql_command_1 = `
	INSERT INTO DAAS_TEMP.GUEST_XREF_BRIDGE_LKP_TMP 
	(
		PRIMARY_ACCOUNT_NBR,
		XREF_ACCOUNT_NBR,
		SOURCE_SYSTEM_NM,
		TIME_ZONE,
		CREATED_DTTM,
		CREATED_BY,
		UPDATED_DTTM,
		UPDATED_BY,
		BATCH_ID,
		MAPPING_SOURCE
    )
	SELECT DISTINCT 
		PRIMARY_ACCOUNT_NBR,
		XREF_ACCOUNT_NBR,
		SOURCE_SYSTEM_NM,
		TIME_ZONE,
		CREATED_DTTM,
		CREATED_BY,
		UPDATED_DTTM,
		UPDATED_BY,
		` + BATCH_ID + `,
		MAPPING_SOURCE
	FROM
	(
		SELECT
			XREF.PRIMARY_ACCOUNT_NBR,
			XREF.XREF_ACCOUNT_NBR,
			XREF.SOURCE_SYSTEM_NM,
			XREF.TIME_ZONE,
			XREF.CREATED_DTTM,
			XREF.CREATED_BY,
			XREF.UPDATED_DTTM,
			XREF.UPDATED_BY,
			MAPPING_SOURCE
		FROM
			DAAS_CORE.GUEST_XREF_BRIDGE_LKP_STREAM XREF
		WHERE NOT EXISTS
		(
			SELECT
				1
			FROM
				DAAS_CORE.TRIP_CUSTOMER_MERGE_QUEUE QUEUE
			WHERE
				XREF.XREF_ACCOUNT_NBR 		= QUEUE.VICTIM_CUSTOMER_ID
			AND XREF.PRIMARY_ACCOUNT_NBR 	= QUEUE.SURVIVOR_CUSTOMER_ID
			AND STATUS = 'PENDING'
		)
		AND XREF.XREF_ACCOUNT_NBR <> XREF.PRIMARY_ACCOUNT_NBR 
        AND XREF.PRIMARY_ACCOUNT_NBR <> '-1'
		AND XREF.METADATA$ACTION = 'INSERT'
	UNION ALL
	SELECT
		XREF.PRIMARY_ACCOUNT_NBR,
		XREF.XREF_ACCOUNT_NBR,
		XREF.SOURCE_SYSTEM_NM,
		XREF.TIME_ZONE,
		XREF.CREATED_DTTM,
		XREF.CREATED_BY,
		XREF.UPDATED_DTTM,
		XREF.UPDATED_BY,
		MAPPING_SOURCE
	FROM
		DAAS_CORE.GUEST_DIM_STREAM DIM
	JOIN 
		DAAS_CORE.GUEST_XREF_BRIDGE_LKP XREF 
	ON
		XREF.XREF_ACCOUNT_NBR = DIM.GUEST_UNIQUE_ID
	AND DIM.DELETE_IND = 'Y'
	AND NOT EXISTS 
	(
		SELECT
			1
		FROM
			DAAS_CORE.TRIP_CUSTOMER_MERGE_QUEUE	QUEUE
		WHERE
			XREF.XREF_ACCOUNT_NBR 		= QUEUE.VICTIM_CUSTOMER_ID
		AND XREF.PRIMARY_ACCOUNT_NBR 	= QUEUE.SURVIVOR_CUSTOMER_ID
		AND STATUS = 'PENDING'
	)
	AND XREF.XREF_ACCOUNT_NBR 		<> XREF.PRIMARY_ACCOUNT_NBR
	AND XREF.PRIMARY_ACCOUNT_NBR 	<> '-1'
	AND DIM.METADATA$ACTION = 'INSERT' 
	)
	`;

	statement1 = snowflake.createStatement( {sqlText: my_sql_command_1 } );
	proc_output = my_sql_command_1;
	statement1.execute();

/*

1) Find out the PRIMARY Account Number From Guest XREF Table and populate table TRIP_CUSTOMER_MERGE_QUEUE with status as PENDING
2) If One Guest Xref is linked with Multiple Primary Account numbers then pick the latest record based on the key sequence value.
3) step 2 is not needed as we are switching to guest xref bridge table and it has the latest records only.
 
*/
	my_sql_command_2 = `
	INSERT INTO	DAAS_CORE.TRIP_CUSTOMER_MERGE_QUEUE
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
	SELECT DISTINCT 
		XREF_ACCOUNT_NBR AS VICTIM_CUSTOMER_ID,
		PRIMARY_ACCOUNT_NBR AS SURVIVOR_CUSTOMER_ID,
		'PENDING' AS STATUS,
		` + BATCH_ID + ` AS BATCH_ID,
		CURRENT_TIMESTAMP()::TIMESTAMP,
		CURRENT_USER(),
		CURRENT_TIMESTAMP()::TIMESTAMP,
		CURRENT_USER(),
		0 AS REPLAY_COUNTER
	FROM
	(
		SELECT XREF.*
		FROM
			DAAS_CORE.GUEST_XREF_BRIDGE_LKP XREF
		JOIN 
			DAAS_TEMP.GUEST_XREF_BRIDGE_LKP_TMP TMP 
		ON
			XREF.XREF_ACCOUNT_NBR 		= TMP.XREF_ACCOUNT_NBR
        AND XREF.PRIMARY_ACCOUNT_NBR 	<> '-1'
		AND XREF.XREF_ACCOUNT_NBR 		<> XREF.PRIMARY_ACCOUNT_NBR
		AND TMP.XREF_ACCOUNT_NBR 		<> TMP.PRIMARY_ACCOUNT_NBR
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
	 )
	`;

	statement2 = snowflake.createStatement( {sqlText: my_sql_command_2 } );
	proc_output = my_sql_command_2;
	statement2.execute();

	snowflake.execute( {sqlText: "COMMIT;" } );


	/* Identify Source count, Core Record Count based on BATCH_ID, LAST_DML_CD */	

	var get_Source_Rec_Count = snowflake.execute( {sqlText: "SELECT COUNT(*) AS source_count FROM DAAS_TEMP.GUEST_XREF_BRIDGE_LKP_TMP;" } );
	get_Source_Rec_Count.next();
	Source_Rec_Count = get_Source_Rec_Count.getColumnValue(1);

	var get_merge_queue_insert_count = snowflake.execute( {sqlText: "SELECT COUNT(VICTIM_CUSTOMER_ID) FROM DAAS_CORE.TRIP_CUSTOMER_MERGE_QUEUE WHERE BATCH_ID = " + BATCH_ID + "" } );
	get_merge_queue_insert_count.next();
	Merge_Queue_Insert_Count = get_merge_queue_insert_count.getColumnValue(1);

	/* Call Update_Batch_Metrics for each inserting each count metrics */
	var call_source_rec_count = snowflake.execute({sqlText: "CALL DAAS_COMMON.BATCH_CONTROL_UPDATE_BATCH_METRICS_PROC('" + BATCH_ID + "' , '" + SHARD_NAME + "', 'Source_Rec_Count', '" + Source_Rec_Count + "');" });
	var call_merge_queue_insert_count = snowflake.execute({sqlText:"CALL DAAS_COMMON.BATCH_CONTROL_UPDATE_BATCH_METRICS_PROC('" + BATCH_ID + "' , '" + SHARD_NAME + "', 'Merge_Queue_Insert_Count' , '" + Merge_Queue_Insert_Count + "');" });

	call_source_rec_count.next();
	call_merge_queue_insert_count.next();

	var get_val_source_count = call_source_rec_count.getColumnValue(1);
	var get_val_merge_queue_insert_count = call_merge_queue_insert_count.getColumnValue(1);

	/* Error Handling if Metrics Update Failed */
	if (get_val_source_count.includes("SUCCESS") != true || get_val_merge_queue_insert_count.includes("SUCCESS") != true) 
	{
		proc_output = "SOURCE COUNT METRIC STATUS: " +  + "\nMERGE QUEUE INSERT COUNT METRIC STATUS: " + get_val_merge_queue_insert_count+ "\nFAILURE RETURNED FROM METRICS";
	}
	else 
	{
		proc_output = "SUCCESS";
	}

	snowflake.execute( {sqlText: "COMMIT;" } );
	
	proc_output = "SUCCESS";
} 
catch (err) 
{  
	proc_output = "FAILURE"; 
	error_code = "Failed: Code: " + err.code + "  State: " + err.state;
	error_message = "\n  Message: " + err.message + "\nStack Trace:\n" + err.stackTraceTxt;
	error_message = error_message.replace(/["'"]/g, "");
					
	if ( proc_step == "Data_Process")
	{
	/* CALL BATCH_CONTROL_UPDATE_BATCH_ERROR_LOG_PROC */
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