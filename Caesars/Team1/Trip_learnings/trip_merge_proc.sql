CREATE OR REPLACE PROCEDURE DAAS_COMMON.TRIP_MERGE_PROC(BATCH_ID FLOAT, SHARD_NAME VARCHAR, WAREHOUSE_NAME VARCHAR, CUSTOM_PARAM1 VARCHAR, CUSTOM_PARAM2 VARCHAR) 
RETURNS VARCHAR NOT NULL 
LANGUAGE JAVASCRIPT 
EXECUTE AS CALLER 
AS 
$$ 
/*
#####################################################################################
Author			: Nambi Arasappan
Purpose			: Identifies trip which needs to be merged based on TRIP_MASTER_ID, TRIP_START_DT, TRIP_END_DT
Input Parameters: BATCH_ID, SHARD_NAME, WAREHOUSE_NAME, CUSTOM_PARAM1, CUSTOM_PARAM2
Output Value	: Success for Successful execution along with DML into TRIP_MASTER and TRIP_DETAIL table and failure for failed execution
Created Date	: 07/08/2022
Version			: 1.0
Modified By		: Surya Jangareddi
Modified Date	: 05/29/2023
Version			: 1.1 (Added step to recreate stream TRIP_MASTER_TRIP_MERGE_CHECK_STREAM to avoid appending repeated records)
Modified Date	: 10/30/2023
Version			: 1.2 TRIP_MASTER_TRIP_MERGE_CHECK_STREAM created in DAAS_TEMP instead of DAAS_CORE during catchup run as App role doesnt have grant to recreate stream
##################################################################################################################################################################################
*/
try
{
	proc_output = "";
	proc_step = "";

	snowflake.execute( {sqlText: "USE WAREHOUSE " + WAREHOUSE_NAME} );
	tag = BATCH_ID + "_TRIP_MERGE_PROC";
	snowflake.execute( {sqlText: "ALTER SESSION SET QUERY_TAG  = '" + tag + "'" });

	var get_Source_Rec_Count = snowflake.execute( {sqlText: "SELECT	COUNT(TRIP_MASTER_ID) FROM DAAS_CORE.TRIP_MERGE_QUEUE_STREAM WHERE METADATA$ACTION='INSERT'" } );
	get_Source_Rec_Count.next();
	Source_Rec_Count = get_Source_Rec_Count.getColumnValue(1);

	if (Source_Rec_Count == 1)
	{
		return "Alert : Only one record in merge queue stream"
	}

	/* Create temp table based on trip merge queue stream */
	snowflake.execute( {sqlText: "CREATE OR REPLACE TABLE DAAS_TEMP.TRIP_MERGE_TEMP_2A AS SELECT * FROM DAAS_CORE.TRIP_MERGE_QUEUE LIMIT 0" } );
	snowflake.execute( {sqlText: "TRUNCATE TABLE DAAS_TEMP.TRIP_RESULT_TMP_MERGE;" } );
	snowflake.execute( {sqlText: "TRUNCATE TABLE DAAS_TEMP.TRIP_RESULT_TMP_MERGE_1;" } );

	snowflake.execute( {sqlText: "BEGIN;" } );
	
	proc_step = "Data_Process";

	/* Loading master id from merge queue into a temp table */
	var my_sql_command_0 = `
	INSERT INTO	DAAS_TEMP.TRIP_MERGE_TEMP_2A
	SELECT DISTINCT 
		TRIP_MASTER_ID,
		STATUS,
		BATCH_ID,
		CREATED_DTTM,
		CREATED_BY,
		UPDATED_DTTM,
		UPDATED_BY,
		NULL
	FROM
		DAAS_CORE.TRIP_MERGE_QUEUE_STREAM TRIP_MERGE_QUEUE
	WHERE
		TRIP_MERGE_QUEUE.METADATA$ACTION = 'INSERT'
	ORDER BY
		TRIP_MASTER_ID ASC
	;`
 
	statement0 = snowflake.createStatement({sqlText: my_sql_command_0});
	statement0.execute();

	/* Fetch input details required for TRIP_FIND_START_DATE_END_DATE_PROC procedure */
	var my_sql_command_1 = `
	INSERT INTO DAAS_TEMP.TRIP_RESULT_TMP_MERGE
	SELECT DISTINCT 
		A.GUEST_UNIQUE_ID,
		NVL(A.PROPERTY_CD,'N/A') AS PROPERTY_CD,
		A.TRIP_TYPE,
		NVL(A.MARKET_CD,'N/A') AS MARKET_CD,
		B.TRANSACTION_TABLE_SK,
		B.TRANSACTION_TABLE,
		B.TRANSACTION_TYPE,
		B.TRANSACTION_START_DTTM,
		B.TRANSACTION_END_DTTM,
		B.BUSINESS_START_DT,
		B.BUSINESS_END_DT,
		B.TRANSACTION_MODIFIED_DTTM,
		'N' AS TRIP_INDICATOR	
	FROM
		DAAS_CORE.TRIP_MASTER A
	JOIN 
		DAAS_CORE.TRIP_DETAIL B
	ON
		A.TRIP_MASTER_ID = B.TRIP_MASTER_ID
	JOIN 
		DAAS_TEMP.TRIP_MERGE_TEMP_2A C
	ON
		A.TRIP_MASTER_ID = C.TRIP_MASTER_ID
	WHERE
		A.DELETE_IND = 'N'
	AND B.TRANSACTION_SUB_TYPE = 'OPENED'
	`;

	statement1 = snowflake.createStatement({sqlText: my_sql_command_1});
	statement1.execute();

	/* Soft delete master in trip master table */
	var my_sql_command_2 = `
	UPDATE DAAS_CORE.TRIP_MASTER TM
	SET
		DELETE_IND 		= 'Y',
		TM.UPDATED_DTTM = CURRENT_TIMESTAMP(),
		TM.UPDATED_BY 	= CURRENT_USER(),
		BATCH_ID 		= ` + BATCH_ID + `,
		LAST_DML_CD 	= 'U'
	FROM
		DAAS_TEMP.TRIP_MERGE_TEMP_2A TBL
	WHERE
		TM.TRIP_MASTER_ID = TBL.TRIP_MASTER_ID
	AND TM.DELETE_IND = 'N'
	;`

	statement2 = snowflake.createStatement({sqlText: my_sql_command_2});
	statement2.execute();

	/* Calling procedure to retrieve updated end date for the new merged trips */
	var my_sql_command_3 = `CALL DAAS_COMMON.TRIP_FIND_START_DATE_END_DATE_PROC(` + BATCH_ID + `,'` + SHARD_NAME + `','DAAS_TEMP.TRIP_RESULT_TMP_MERGE','DAAS_TEMP.TRIP_RESULT_TMP_MERGE_1');`;	

	statement3 = snowflake.createStatement( {sqlText: my_sql_command_3} );
	statment_trip_result1 = statement3.execute();
	statment_trip_result1.next(); 

	var out1 = statment_trip_result1.getColumnValue(1);
	if(out1 == "FAILURE") return "FAILURE_FROM_TRIP_FUNCTION";

	result_set_3 = snowflake.execute( {sqlText: "SELECT COUNT(*) FROM DAAS_TEMP.TRIP_RESULT_TMP_MERGE_1" } );
	result_set_3.next();
	var get_temp_row_count = result_set_3.getColumnValue(1);

	if( get_temp_row_count != 0)
	{
		/* Insert merged records into trip master based on procedure output table */
		var my_sql_command_4 = `
		INSERT INTO	DAAS_CORE.TRIP_MASTER
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
			DAAS_CORE.TRIP_MASTER_SEQ.NEXTVAL  TRIP_MASTER_ID,
			GUEST_UNIQUE_ID,
			IFF(TRIP_TYPE = 'PROPERTY',PROPERTY_CD,'N/A') PROPERTY_CD,
			IFF(TRIP_TYPE = 'MARKET',MARKET_CD,'N/A') MARKET_CD,
			TRIP_TYPE         ,
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
			SELECT DISTINCT 
				TRIP_START_DT,
				TRIP_END_DT,
				TRIP_TYPE,
				PROPERTY_CD,
				MARKET_CD,
				GUEST_UNIQUE_ID 
			FROM 
				DAAS_TEMP.TRIP_RESULT_TMP_MERGE_1 RT 
		)
		`;

		statement4 = snowflake.createStatement({sqlText: my_sql_command_4});
		statement4.execute();

		var my_sql_command_6 = `
		INSERT INTO	DAAS_CORE.TRIP_DETAIL
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
			TRIP_MASTER_ID AS TRIP_MASTER_ID,
			TRANSACTION_TABLE_SK,
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
			SELECT DISTINCT 
				RT.*,
				TRIP_MASTER_ID 
			FROM 
				DAAS_TEMP.TRIP_RESULT_TMP_MERGE_1 RT 
			JOIN 
				DAAS_CORE.TRIP_MASTER TM  
			ON 
				RT.GUEST_UNIQUE_ID 	= TM.GUEST_UNIQUE_ID 
			AND TM.TRIP_TYPE 		= RT.TRIP_TYPE 
			AND TM.PROPERTY_CD 		= RT.PROPERTY_CD
			AND TM.MARKET_CD 		= RT.MARKET_CD
			AND TM.DELETE_IND 		= 'N' 
			AND RT.BUSINESS_START_DT BETWEEN TM.TRIP_START_DT AND TM.TRIP_END_DT
		)
		ORDER BY 
			TRIP_DETAIL_ID ASC, TRIP_MASTER_ID ASC
		;`
	
		statement6 = snowflake.createStatement({sqlText: my_sql_command_6});
		statement6.execute();
	}

	/* Insert into merged processed table that the master ID has been processed from merge queue stream into master/detail table */

	var my_sql_command_7 = `
	INSERT INTO DAAS_CORE.TRIP_MERGE_PROCESSED
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
		C.TRIP_MASTER_ID,
		'PROCESSED',
		` + BATCH_ID + `,
		CURRENT_TIMESTAMP(),
		CURRENT_USER(),
		CURRENT_TIMESTAMP(),
		CURRENT_USER()
	FROM
		DAAS_TEMP.TRIP_MERGE_TEMP_2A C
	;`

	statement7 = snowflake.createStatement({sqlText: my_sql_command_7});
	statement7.execute();
	
	snowflake.execute( {sqlText: "COMMIT;" } );

	proc_step = "Update_Metrics";

	/* Identify Core Record_Count based on batch_id, LAST_DML_CD */
	var get_Master_Count = snowflake.execute( {sqlText: "SELECT NVL(SUM(CASE WHEN LAST_DML_CD = 'U' THEN 1 ELSE 0 END), 0) AS UPDATE_COUNT,  NVL(SUM(CASE WHEN LAST_DML_CD = 'I' THEN 1 ELSE 0 END), 0) AS INSERT_COUNT FROM DAAS_CORE.TRIP_MASTER WHERE BATCH_ID = " + BATCH_ID + "" } );
	get_Master_Count.next();

	Master_Update_Count = get_Master_Count.getColumnValue(1);
	Master_Insert_Count = get_Master_Count.getColumnValue(2);

	var get_Detail_Count = snowflake.execute( {sqlText: "SELECT NVL(SUM(CASE WHEN LAST_DML_CD = 'U' THEN 1 ELSE 0 END), 0) AS UPDATE_COUNT,  NVL(SUM(CASE WHEN LAST_DML_CD = 'I' THEN 1 ELSE 0 END), 0) AS INSERT_COUNT FROM DAAS_CORE.TRIP_DETAIL WHERE BATCH_ID = " + BATCH_ID + "" } );
	get_Detail_Count.next();

	Detail_Update_Count = get_Detail_Count.getColumnValue(1);
	Detail_Insert_Count = get_Detail_Count.getColumnValue(2);

	/* Call Update_Batch_Metrics for each inserting each count metrics */

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

	/* Error Handling if Metrics Update Failed */
	if (get_val_source_count.includes("SUCCESS") != true || 
		get_val_master_insert_count.includes("SUCCESS") != true || 
		get_val_master_update_count.includes("SUCCESS") != true || 
		get_val_detail_insert_count.includes("SUCCESS") != true || 
		get_val_detail_update_count.includes("SUCCESS") != true) 
	{ 
		proc_output = "SOURCE COUNT METRIC STATUS: " + get_val_source_count + "\nMASTER INSERT COUNT METRIC STATUS: " + get_val_master_insert_count + + "\nMASTER UPDATE COUNT METRIC STATUS: " + get_val_master_update_count +"\nDETAIL INSERT COUNT METRIC STATUS: " + get_val_detail_insert_count + "\nDETAIL UPDATE COUNT METRIC STATUS: " + get_val_detail_update_count +"\nFAILURE RETURNED FROM METRICS";
	}
	else 
	{ 
		proc_output = "SUCCESS";
	}

	/* Commit the Update_Metrics Step */
	snowflake.execute( {sqlText: "COMMIT;" } ); 
	
	/* Recreate stream TRIP_MASTER_TRIP_MERGE_CHECK_STREAM to avoid appending repeated records */
	snowflake.execute( {sqlText: "CREATE OR REPLACE STREAM DAAS_TEMP.TRIP_MASTER_TRIP_MERGE_CHECK_STREAM ON TABLE DAAS_CORE.TRIP_MASTER;" } );

	var my_sql_command_31 = "";

	proc_output = "SUCCESS";
}

catch (err) 
{ 
	if(err.message == "FAILURE_FROM_TRIP_FUNCTION") 
	{
		proc_output = "FAILURE_FROM_TRIP_FUNCTION"; 
		snowflake.execute( {sqlText: "ROLLBACK;" } );
	}
	else if (err.message=="Alert : Only one record in merge queue stream")
	{
		proc_output =  "Alert : Only one record in merge queue stream";
		snowflake.execute( {sqlText: "ROLLBACK;" } );
	}
	else {proc_output = "FAILURE";};
	
	error_code = "Failed: Code: " + err.code + "  State: " + err.state;
	error_message = "\n  Message: " + err.message + "\nStack Trace:\n" + err.stackTraceTxt;
	error_message = error_message.replace(/["'"]/g, "");
	
	if ( proc_step == "Data_Process")
	{
	/* CALL BATCH_CONTROL_UPDATE_BATCH_ERROR_LOG_PROC */
	
		snowflake.execute( {sqlText: "ROLLBACK;" } );
		my_sql_command_31 = "CALL DAAS_COMMON.BATCH_CONTROL_UPDATE_BATCH_ERROR_LOG_PROC ('" + BATCH_ID + "','" + SHARD_NAME + "','" + error_code + "','" + error_message + "','','','FATAL','" + tag + "_" + proc_step +"')"	
	}
	else 
	{ 
		snowflake.execute( {sqlText: "ROLLBACK;" } );
		my_sql_command_31 = "CALL DAAS_COMMON.BATCH_CONTROL_UPDATE_BATCH_ERROR_LOG_PROC ('" + BATCH_ID + "','" + SHARD_NAME + "','" + error_code + "','" + error_message + "','','','INFORMATIONAL','" + tag + "_" + proc_step +"')"
	} 

	snowflake.execute( {sqlText: my_sql_command_31});
}
return proc_output;
$$;