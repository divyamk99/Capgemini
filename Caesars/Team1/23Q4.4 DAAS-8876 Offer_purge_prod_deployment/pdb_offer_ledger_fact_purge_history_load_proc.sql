CREATE OR REPLACE PROCEDURE DAAS_COMMON.PDB_OFFER_LEDGER_FACT_PURGE_HISTORY_LOAD_PROC(BATCH_ID FLOAT, SHARD_NAME VARCHAR, WAREHOUSE_NAME VARCHAR, CUSTOM_PARAM1 VARCHAR, CUSTOM_PARAM2 VARCHAR)
RETURNS VARCHAR NOT NULL
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER 
AS
$$
var proc_output = "";

/*
#####################################################################################
Author			: Nikhil Katariya
Purpose			: Load history data from DAAS_CORE.OFFER_LEDGER_FACT table to DAAS_CORE.OFFER_LEDGER_FACT_HISTORY table
Input Parameters: None
Output Value	: SUCCESS for successful execution and FAILURE for failed execution 
Created Date	: 09/16/2022
Modified By		: Surya Jangareddi
Modified Date	: 06/19/2023 
Version			: 1.0
Version			: 1.1 [Replaced Merge to Insert]
#####################################################################################
*/

try
{ 
	snowflake.execute( {sqlText: "BEGIN;" } );
	
	/* To get session_query_tag by concatenating batch_id and procedure name */
	snowflake.execute( {sqlText: "USE WAREHOUSE " + WAREHOUSE_NAME + " " } );
	
	var query_tag = snowflake.execute( {sqlText: "SELECT CONCAT('" + BATCH_ID + "','_PDB_OFFER_LEDGER_FACT_PURGE_HISTORY_LOAD_PROC')" } );
	query_tag.next();
	var tag = query_tag.getColumnValue(1);

	snowflake.execute( {sqlText: "ALTER SESSION SET QUERY_TAG = '" + tag + "'" } );

	var Proc_Step = "";
	Proc_Step = "Data_Process";

	var get_Source_Rec_Count = snowflake.execute( {sqlText:"SELECT COUNT(*) FROM DAAS_CORE.OFFER_LEDGER_FACT WHERE (PURGE_FLG = 1 OR DELETE_IND = 'Y' OR CURRENT_STATUS_IND = 'N')"} );
	get_Source_Rec_Count.next();
	Source_Rec_Count = get_Source_Rec_Count.getColumnValue(1);

	/* Insert data into OFFER_LEDGER_FACT_HISTORY from OFFER_LEDGER_FACT */
	
	var my_sql_command_1 = `
INSERT INTO DAAS_CORE.OFFER_LEDGER_FACT_HISTORY
    
	(
		OFFER_LEDGER_FACT_SK,
		GUEST_OFFER_NBR,
		GUEST_UNIQUE_ID,
		OFFER_ID,
		COLLATERAL_ID,
		COUPON_TYPE,
		COUPON_ID,
		VALID_PROPERTY_CD,
		OFFER_STATUS_CD,
		PROPERTY_ID,
		GUEST_ID,
		OFFER_STATUS_DESC,
		REDEMPTION_DT,
		CURRENT_STATUS_IND,
		RECIPIENT_GROUP_ID,
		OFFER_STATUS_DT,
		OFFER_SENT_DT,
		REDEEMED_COUPON_CNT,
		PARADB_LIST_ID,
		MAIL_ID,
		SOURCE_UPDATED_TIMESTAMP,
		REDEEM_AMT,
		REDEEM_AMOUNT_SOURCE_CD,
		REDEEM_CHANNEL_CD,
		PDB_TRIP_ID,
		COUPON_KEY,
		SOURCE_UPDATED_BY,
		SOURCE_UPDATED_TIMESTAMP_REDEEMED,
		OFFER_WORTH,
		PURGE_DT,
		PURGE_FLG,
		SOURCE_SYSTEM_NM,
		TIME_ZONE,
		GST_OFFER_SENT_RAW_BATCH_ID,
		GST_OFFER_RSV_RDM_RAW_BATCH_ID,
		OFFER_LEDGER_FACT_BATCH_ID,
		BATCH_ID,
		CREATED_DTTM,
		CREATED_BY,
		UPDATED_DTTM,
		UPDATED_BY,
		DELETE_IND,
		LAST_DML_CD,
		REDEEM_AMT_COST_ADJUSTED,
		REPLAY_COUNTER
	)
	SELECT
		OFFER_LEDGER_FACT_SK,
		GUEST_OFFER_NBR,
		GUEST_UNIQUE_ID,
		OFFER_ID,
		COLLATERAL_ID,
		COUPON_TYPE,
		COUPON_ID,
		VALID_PROPERTY_CD,
		OFFER_STATUS_CD,
		PROPERTY_ID,
		GUEST_ID,
		OFFER_STATUS_DESC,
		REDEMPTION_DT,
		CURRENT_STATUS_IND,
		RECIPIENT_GROUP_ID,
		OFFER_STATUS_DT,
		OFFER_SENT_DT,
		REDEEMED_COUPON_CNT,
		PARADB_LIST_ID,
		MAIL_ID,
		SOURCE_UPDATED_TIMESTAMP,
		REDEEM_AMT,
		REDEEM_AMOUNT_SOURCE_CD,
		REDEEM_CHANNEL_CD,
		PDB_TRIP_ID,
		COUPON_KEY,
		SOURCE_UPDATED_BY,
		SOURCE_UPDATED_TIMESTAMP_REDEEMED,
		OFFER_WORTH,
		PURGE_DT,
		PURGE_FLG,
		SOURCE_SYSTEM_NM,
		TIME_ZONE,
		GST_OFFER_SENT_RAW_BATCH_ID,
		GST_OFFER_RSV_RDM_RAW_BATCH_ID,
		BATCH_ID,
		`+BATCH_ID+`,
		CREATED_DTTM,
		CREATED_BY,
		UPDATED_DTTM,
		UPDATED_BY,
		DELETE_IND,
		'I',
		REDEEM_AMT_COST_ADJUSTED,
		REPLAY_COUNTER
	FROM
		DAAS_CORE.OFFER_LEDGER_FACT
	WHERE 
	(OFFER_LEDGER_FACT.PURGE_FLG = 1 OR OFFER_LEDGER_FACT.DELETE_IND = 'Y' OR OFFER_LEDGER_FACT.CURRENT_STATUS_IND = 'N')
	;`
        
	var statement1 = snowflake.createStatement( {sqlText: my_sql_command_1 } );
	statement1.execute();
		
		
	
	var del_cmd = `DELETE FROM DAAS_CORE.OFFER_LEDGER_FACT WHERE (PURGE_FLG = 1 OR DELETE_IND = 'Y' OR CURRENT_STATUS_IND = 'N')`;
    var del_statement = snowflake.createStatement({sqlText:del_cmd}).execute();

	/*	The below step checks for all the latest records in the history table if there is a recent record in the Fact. 
	If there is a latest record in the fact, Then the current status in the history = 'N' for deleted records. 
	If there is no latest record in the fact update current_status_ind = 'Y' in the history */
	
	var my_sql_command_2 = `
	CREATE OR REPLACE TABLE DAAS_TEMP.OFFER_LEDGER_FACT_HISTORY_TEMP_CURR_FLG AS 
	SELECT 
		D.OFFER_LEDGER_FACT_SK, 
		(CASE WHEN F.GUEST_UNIQUE_ID IS NULL THEN 'Y' ELSE 'N' END) AS NEW_CURRENT_STATUS_IND 
		--D.*,F.OFFER_LEDGER_FACT_SK,F.GUEST_OFFER_NBR, F.GUEST_UNIQUE_ID,F.OFFER_ID,F.COLLATERAL_ID,F.COUPON_TYPE,F.COUPON_KEY,F.OFFER_STATUS_CD,F.CURRENT_STATUS_IND 
	FROM 
	(
		SELECT * 
		FROM 
		(
			SELECT 
				OFFER_LEDGER_FACT_SK, 
				GUEST_OFFER_NBR, 
				GUEST_UNIQUE_ID,
				OFFER_ID, 
				COLLATERAL_ID,
				COUPON_TYPE, 
				COUPON_KEY,
				COUPON_ID,
				OFFER_STATUS_CD,
				VALID_PROPERTY_CD,
				OFFER_STATUS_DT, 
				UPDATED_DTTM, 
				RANK() OVER(PARTITION BY GUEST_OFFER_NBR,OFFER_ID,GUEST_UNIQUE_ID,COLLATERAL_ID,COUPON_TYPE, COUPON_ID,VALID_PROPERTY_CD ORDER BY UPDATED_DTTM DESC) RNK, 
				DELETE_IND, 
				CURRENT_STATUS_IND, 
				PURGE_FLG
			FROM 
				DAAS_CORE.OFFER_LEDGER_FACT_HISTORY
		)H 
		WHERE 
			H.DELETE_IND = 'Y' 
		AND H.RNK = 1 
		AND PURGE_FLG = 0
	)D 
	LEFT JOIN 
		DAAS_CORE.OFFER_LEDGER_FACT F
	ON F.GUEST_OFFER_NBR = D.GUEST_OFFER_NBR
	AND F.GUEST_UNIQUE_ID = D.GUEST_UNIQUE_ID
	AND F.OFFER_ID = D.OFFER_ID
	AND F.COLLATERAL_ID = D.COLLATERAL_ID
	AND F.COUPON_TYPE = D.COUPON_TYPE
	AND F.COUPON_ID = D.COUPON_ID
	AND F.VALID_PROPERTY_CD = D.VALID_PROPERTY_CD
	;`
	
	var statement2 = snowflake.createStatement( {sqlText: my_sql_command_2 } );
	statement2.execute();


/*	For the business key in history table, if there is no recent record in the Fact, it updates current_status_ind = 'Y'*/
	var my_sql_command_3 = `
	UPDATE DAAS_CORE.OFFER_LEDGER_FACT_HISTORY HIST
	SET 
		HIST.CURRENT_STATUS_IND = TMP.NEW_CURRENT_STATUS_IND, 
		HIST.BATCH_ID = `+BATCH_ID+`
	FROM 
		DAAS_TEMP.OFFER_LEDGER_FACT_HISTORY_TEMP_CURR_FLG TMP
	WHERE 
		TMP.OFFER_LEDGER_FACT_SK = HIST.OFFER_LEDGER_FACT_SK
	;`
	
	
	var statement3 = snowflake.createStatement( {sqlText: my_sql_command_3 } );
	statement3.execute();


	proc_output = "SUCCESS";

	/* Commit the Data Process Step */
	snowflake.execute( {sqlText: "COMMIT;" } );

	Proc_Step = "Update_Metrics";

	/* Identify Core Record_Count based on BATCH_ID */
	var get_Core_Count = snowflake.execute( {sqlText: "SELECT COUNT(*) FROM DAAS_CORE.OFFER_LEDGER_FACT_HISTORY WHERE BATCH_ID = "+BATCH_ID+""});
	get_Core_Count.next();
	Core_Insert_Count = get_Core_Count.getColumnValue(1); 

	/* Call Update_Batch_Metrics for each inserting each count metrics */
	var call_source_rec_count = snowflake.execute({sqlText: "CALL DAAS_COMMON.BATCH_CONTROL_UPDATE_BATCH_METRICS_PROC('" + BATCH_ID + "' , '" + SHARD_NAME + "','Source_Rec_Count','" + Source_Rec_Count + "');" });
	var call_core_insert_count = snowflake.execute({sqlText: "CALL DAAS_COMMON.BATCH_CONTROL_UPDATE_BATCH_METRICS_PROC('" + BATCH_ID + "' , '" + SHARD_NAME + "','Core_Insert_Count','" + Core_Insert_Count + "');" }); 

	call_source_rec_count.next();
	call_core_insert_count.next();

	var get_val_source_count = call_source_rec_count.getColumnValue(1);
	var get_val_core_insert_count = call_core_insert_count.getColumnValue(1);

	/* Error Handling if Metrics Update Failed */
	if (get_val_source_count.includes("SUCCESS") != true || get_val_core_insert_count.includes("SUCCESS") != true) 
	{ 
		proc_output = "SOURCE COUNT METRIC STATUS: " + get_val_source_count + "\nRAW INSERT COUNT METRIC STATUS: " + get_val_core_insert_count + "\nFAILURE RETURNED FROM METRICS";
	}
	else 
	{
	proc_output = "SUCCESS";
	} 
	
	/* Commit the Update_Metrics Step */
	snowflake.execute( {sqlText: "COMMIT;" } );
}

/* Below exception Handling Code can be reused in all stg to raw procedures as it is not specific to a procedure */
catch (err) 
{ 
	proc_output = "FAILURE";

	error_code = "Failed: Code: " + err.code + "  State: " + err.state;
	error_message = "\n  Message: " + err.message + "\nStack Trace:\n" + err.stackTraceTxt;
	error_message = error_message.replace(/["']/g, "");

if (Proc_Step == "Data_Process") 
{
	snowflake.execute( {sqlText: "ROLLBACK;" } )
	/*CALL UPDATE_BATCH_ERROR_LOG*/
	var sql_cmd = snowflake.execute( {sqlText:"CALL DAAS_COMMON.BATCH_CONTROL_UPDATE_BATCH_ERROR_LOG_PROC ('" + BATCH_ID + "','" + SHARD_NAME + "','" + error_code + "','" + error_message + "','','','FATAL','" + tag + "' || '_' || '" + Proc_Step +"')" });
}
else 
{
	snowflake.execute( {sqlText: "COMMIT;" } );
	/*CALL UPDATE_BATCH_ERROR_LOG*/
	var sql_cmd = snowflake.execute( {sqlText:"CALL DAAS_COMMON.BATCH_CONTROL_UPDATE_BATCH_ERROR_LOG_PROC ('" + BATCH_ID + "','" + SHARD_NAME + "','" + error_code + "','" + error_message + "','','',' INFORMATIONAL',  '" + tag + "' || '_' || '" + Proc_Step +"')" });
} 
} 
return proc_output;
$$;