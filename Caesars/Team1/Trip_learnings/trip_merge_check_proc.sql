CREATE OR REPLACE PROCEDURE DAAS_COMMON.TRIP_MERGE_CHECK_PROC( 
BATCH_ID FLOAT,
SHARD_NAME VARCHAR,
WAREHOUSE_NAME VARCHAR,
CUSTOM_PARAM1 VARCHAR,
CUSTOM_PARAM2 VARCHAR) 
RETURNS VARCHAR NOT NULL 
LANGUAGE javascript 
EXECUTE AS CALLER AS 
$$ 
/*
#####################################################################################
AUTHOR: NAMBI ARASAPPAN
PURPOSE: IDENTIFIES TRIP WHICH NEEDS TO BE MERGED BASED ON MASTER ID, TRIP START DT, TRIP END DT
INPUT PARAMETERS: BATCH_ID, SHARD_NAME, WAREHOUSE_NAME, CUSTOM_PARAM1, CUSTOM_PARAM2
OUTPUT VALUE: SUCCESS FOR SUCCESSFUL EXECUTION ALONG WITH DML INTO TRIP MASTER AND TRIP DETAIL TABLE AND FAILURE FOR FAILED EXECUTION
CREATE DATE: 07/13/2022
VERSION: 1.0 
MODIFIED DATE: 09/07/2022
VERSION:1.1(CHANGES IMPLEMENTED TO PROCESS HOTEL RECORDS WITH NO MAX TRIP DAY RULE)
MODIFIED BY: NAMBI ARASAPPAN
MODIFIED DATE: 09/16/2022
VERSION:1.2 CHANGES IMPLEMENTED TO PROCESS THE FOLLOWING SCENARIO:
			26-02 TMP HOTEL
			27-27 TM NON HOTEL
RECORD IS NOT INSERTED INTO MERGE QUEUE WHEN TM IS NOT AVAILABLE IN STREAM. 
ADDED "TM.TRIP_START_DT BETWEEN TMP.TRIP_START_DT AND TMP.TRIP_END_DT" CONDITION TO HANDLE SUCH SCENARIO.
MODIFIED BY: NAMBI ARASAPPAN
VERSION:1.3 CHANGES IMPLEMENTED TO PROCESS THE FOLLOWING SCENARIO:
			26-02 TMP HOTEL
            27-27 TM NON HOTEL
			
			26-02 TMP NON HOTEL
            27-27 TM  HOTEL
RECORD IS NOT INSERTED INTO MERGE QUEUE WHEN TM IS NOT AVAILABLE IN STREAM. 
ADDED "TM.TRIP_START_DT BETWEEN TMP.TRIP_START_DT AND TMP.TRIP_END_DT" CONDITION TO HANDLE SUCH SCENARIO.
MODIFIED BY: NAMBI ARASAPPAN
MODIFIED DATE: 11/07/2022
MODIFIED DATE: 10/30/2023
Version	:1.3 TRIP_MASTER_TRIP_MERGE_CHECK_STREAM created in DAAS_TEMP instead of DAAS_CORE during catchup run as App role doesnt have grant to recreate stream
#####################################################################################
*/
try
{
proc_output = "";

proc_step = "";

snowflake.execute( {sqlText: "USE WAREHOUSE " + WAREHOUSE_NAME} );

tag = BATCH_ID + "_TRIP_MERGE_CHECK_PROC";

snowflake.execute( {sqlText: "ALTER SESSION SET QUERY_TAG  = '" + tag + "'" });

var get_Source_Rec_Count = snowflake.execute( {sqlText: "SELECT	COUNT(TRIP_MASTER_ID) FROM DAAS_TEMP.TRIP_MASTER_TRIP_MERGE_CHECK_STREAM WHERE METADATA$ACTION='INSERT'" } );

get_Source_Rec_Count.next();

Source_Rec_Count = get_Source_Rec_Count.getColumnValue(1);

snowflake.execute( {sqlText: "CREATE OR REPLACE TABLE DAAS_TEMP.TRIP_MERGE_TEMP AS SELECT * FROM DAAS_CORE.TRIP_MASTER LIMIT 0" } );

snowflake.execute( {sqlText: "CREATE OR REPLACE TABLE DAAS_TEMP.TRIP_MERGE_TEMP_1 AS SELECT NULL AS TRIP_GAP_DAYS, NULL AS MAX_TRIP_DAYS,* FROM DAAS_CORE.TRIP_MASTER LIMIT 0" } );

snowflake.execute( {sqlText: "CREATE OR REPLACE TABLE DAAS_TEMP.TRIP_MERGE_TEMP_2 AS SELECT NULL AS TRIP_GAP_DAYS, NULL AS MAX_TRIP_DAYS,0::NUMBER(38,0) AS TEMP_MASTER_ID ,* FROM DAAS_CORE.TRIP_MASTER LIMIT 0" } );

snowflake.execute( {sqlText: "BEGIN;" } );

proc_step = "Data_Process";

/*Loads data into temp table from trip master stream as soon as there is new data in trip master table*/
var my_sql_command_1 =
`INSERT
	INTO
	DAAS_TEMP.TRIP_MERGE_TEMP 
	SELECT
	TRIP_MASTER_ID,
	GUEST_UNIQUE_ID,
	IFF(TRIP_TYPE = 'PROPERTY',PROPERTY_CD,'') PROPERTY_CD,
	IFF(TRIP_TYPE = 'MARKET',MARKET_CD,'') MARKET_CD,
	TRIP_TYPE,
	TRIP_START_DT,
	TRIP_END_DT,
	DELETE_IND,
	CREATED_DTTM,
	CREATED_BY,
	UPDATED_DTTM,
	UPDATED_BY,
	BATCH_ID,
	REPLAY_COUNTER,
	LAST_DML_CD
FROM
	DAAS_TEMP.TRIP_MASTER_TRIP_MERGE_CHECK_STREAM TRIP_MASTER_TRIP_MERGE_CHECK_STREAM
WHERE
	TRIP_MASTER_TRIP_MERGE_CHECK_STREAM.METADATA$ACTION = 'INSERT' AND TRIP_MASTER_TRIP_MERGE_CHECK_STREAM.DELETE_IND = 'N'
ORDER BY
	TRIP_MASTER_ID ASC;`

statement1 = snowflake.createStatement({sqlText: my_sql_command_1});

statement1.execute();

result_set_0 = snowflake.execute( {sqlText: "SELECT COUNT(*) FROM DAAS_TEMP.TRIP_MERGE_TEMP" } );

result_set_0.next();

var get_row_count = result_set_0.getColumnValue(1);

if ( get_row_count != 0 ) {

result_set_2 = snowflake.execute( {sqlText: "SELECT VALUE,* FROM DAAS_CORE.TRIP_CONFIG WHERE CONFIG_SUBTYPE = 'ENTERPRISE' AND KEY = 'MAX_TRIP_DAYS' AND ACTIVE_FLG = 'Y'" } );

result_set_2.next();

result_set_2_A = snowflake.execute( {sqlText: "SELECT VALUE,* FROM DAAS_CORE.TRIP_CONFIG WHERE CONFIG_SUBTYPE = 'ENTERPRISE' AND KEY = 'TRIP_GAP_DAYS' AND ACTIVE_FLG = 'Y'" } );

result_set_2_A.next();

var max_trip_days = result_set_2.getColumnValue(1);

var trip_gap_days = result_set_2_A.getColumnValue(1);

/*Uses config function to find trip gap days and max trip days for each property/market/enterprise record and loads them into another temp table*/
var my_sql_command_2 = 
`INSERT
	INTO
	DAAS_TEMP.TRIP_MERGE_TEMP_1 
	SELECT
	CASE
		WHEN TRIP_TYPE = 'PROPERTY' THEN DAAS_COMMON.TRIP_CONFIG_UDF_PROPERTY('TRIP_GAP_DAYS',PROPERTY_CD,TRIP_START_DT) 
		WHEN TRIP_TYPE = 'MARKET' THEN DAAS_COMMON.TRIP_CONFIG_UDF_MARKET('TRIP_GAP_DAYS',MARKET_CD,TRIP_START_DT)
		ELSE `+ trip_gap_days +` 
		END,
	CASE
		WHEN TRIP_TYPE = 'PROPERTY' THEN DAAS_COMMON.TRIP_CONFIG_UDF_PROPERTY('MAX_TRIP_DAYS',PROPERTY_CD,TRIP_START_DT) 
		WHEN TRIP_TYPE = 'MARKET' THEN DAAS_COMMON.TRIP_CONFIG_UDF_MARKET('MAX_TRIP_DAYS',MARKET_CD,TRIP_START_DT) 
		ELSE `+ max_trip_days +` 
		END,
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
		REPLAY_COUNTER,
		LAST_DML_CD
		FROM
			DAAS_TEMP.TRIP_MERGE_TEMP TMP
		WHERE
			TMP.DELETE_IND = 'N'
		ORDER BY
			TRIP_MASTER_ID ASC;`
	
statement2 = snowflake.createStatement({sqlText: my_sql_command_2});

statement2.execute();
}
result_set_1 = snowflake.execute( {sqlText: "SELECT COUNT(*) FROM DAAS_TEMP.TRIP_MERGE_TEMP_1" } );

result_set_1.next();

var get_row_count_2 = result_set_1.getColumnValue(1);

if ( get_row_count_2 != 0 ) {
/*Loads trip merge candidates after comparing trip gap days, trip start date and end dates of new records and exsisiting records and skips max trip day rule for HOTEL scenario*/
var my_sql_command_3 = 
`  INSERT
	INTO
	DAAS_TEMP.TRIP_MERGE_TEMP_2 
	SELECT
    'N/A' AS TRIP_GAP_DAYS,
    'N/A' AS MAX_TRIP_DAYS,
    TMP.TRIP_MASTER_ID AS TEMP_MASTER_ID,
    TM.TRIP_MASTER_ID,
    TM.GUEST_UNIQUE_ID,
    TM.PROPERTY_CD,
    TM.MARKET_CD,
    TM.TRIP_TYPE,
    TM.TRIP_START_DT,
    TM.TRIP_END_DT,
    TM.DELETE_IND,
    TM.CREATED_DTTM,
    TM.CREATED_BY,
    TM.UPDATED_DTTM,
    TM.UPDATED_BY,
    TM.BATCH_ID,
    TM.REPLAY_COUNTER,
    TM.LAST_DML_CD
FROM
    DAAS_TEMP.TRIP_MERGE_TEMP_1 TMP    
JOIN DAAS_CORE.TRIP_MASTER TM
ON
    TM.GUEST_UNIQUE_ID = TMP.GUEST_UNIQUE_ID
   AND IFF(TM.TRIP_TYPE = 'PROPERTY',TM.PROPERTY_CD,'')  = TMP.PROPERTY_CD
   AND IFF(TM.TRIP_TYPE = 'MARKET',TM.MARKET_CD,'') = TMP.MARKET_CD
   AND TM.TRIP_TYPE = TMP.TRIP_TYPE
    LEFT JOIN     
    (
        SELECT DISTINCT
            TRIP_MASTER_ID
        FROM
            DAAS_CORE.TRIP_DETAIL
        WHERE
            DELETE_IND = 'N' AND TRANSACTION_TYPE = 'HOTEL'
    ) TM_HOTEL
    ON
        TM.TRIP_MASTER_ID = TM_HOTEL.TRIP_MASTER_ID
    WHERE
        TM.TRIP_MASTER_ID <> TEMP_MASTER_ID
    AND TM.DELETE_IND = 'N'
    AND TMP.DELETE_IND = 'N'
    AND (
            CASE
            WHEN /* NON HOTEL RECORDS */
                TM_HOTEL.TRIP_MASTER_ID IS NULL
            THEN ((TMP.TRIP_END_DT - TMP.TRIP_START_DT + NVL(TMP.TRIP_GAP_DAYS,`+ trip_gap_days +`) < NVL(TMP.MAX_TRIP_DAYS,`+ max_trip_days+`))
			AND (DATEADD(DAY,NVL(TMP.TRIP_GAP_DAYS,`+ trip_gap_days +`),TMP.TRIP_END_DT) BETWEEN TM.TRIP_START_DT AND TM.TRIP_END_DT))
                OR (TMP.TRIP_START_DT BETWEEN TM.TRIP_START_DT AND TM.TRIP_END_DT)
				OR (TM.TRIP_START_DT BETWEEN TMP.TRIP_START_DT AND TMP.TRIP_END_DT) 
            ELSE /* HOTEL RECORDS */
                (DATEADD(DAY,NVL(TMP.TRIP_GAP_DAYS,`+ trip_gap_days +`),TMP.TRIP_END_DT) BETWEEN TM.TRIP_START_DT AND TM.TRIP_END_DT)
                    OR (TMP.TRIP_START_DT BETWEEN TM.TRIP_START_DT AND TM.TRIP_END_DT)
					OR (TM.TRIP_START_DT BETWEEN TMP.TRIP_START_DT AND TMP.TRIP_END_DT)   
            END
    )
ORDER BY
    TM.TRIP_MASTER_ID ASC;`
	
statement3 = snowflake.createStatement({sqlText: my_sql_command_3});

statement3.execute();
}
result_set_3 = snowflake.execute( {sqlText: "SELECT COUNT(*) FROM DAAS_TEMP.TRIP_MERGE_TEMP_2" } );

result_set_3.next();

var get_temp_row_count = result_set_3.getColumnValue(1);

/*Loads distinct trip merge candidates from temp column into the queue table*/
if( get_temp_row_count != 0){
var my_sql_command_4 =
`INSERT INTO DAAS_CORE.TRIP_MERGE_QUEUE(
	TRIP_MASTER_ID,
	STATUS,
	BATCH_ID,
	CREATED_DTTM,
	CREATED_BY,
	UPDATED_DTTM,
	UPDATED_BY,
	REPLAY_COUNTER
)
SELECT
	(TMP.TEMP_MASTER_ID),
	'PENDING',
	` + BATCH_ID + `,
	CURRENT_TIMESTAMP(),
	CURRENT_USER(),
	CURRENT_TIMESTAMP(),
	CURRENT_USER(),
	NULL
FROM 
	DAAS_TEMP.TRIP_MERGE_TEMP_2 TMP
UNION
	SELECT
	(TMP.TRIP_MASTER_ID),
	'PENDING',
	` + BATCH_ID + `,
	CURRENT_TIMESTAMP(),
	CURRENT_USER(),
	CURRENT_TIMESTAMP(),
	CURRENT_USER(),
	NULL
FROM 
	DAAS_TEMP.TRIP_MERGE_TEMP_2 TMP
ORDER BY
	TEMP_MASTER_ID ASC;`
	
statement4 = snowflake.createStatement({sqlText: my_sql_command_4});

statement4.execute();

}

snowflake.execute( {sqlText: "COMMIT;" } );

/*Identify Core Record_Count based on batch_id, LAST_DML_CD*/
var get_merge_queue_insert_count = snowflake.execute( {sqlText: "SELECT COUNT(TRIP_MASTER_ID) FROM DAAS_CORE.TRIP_MERGE_QUEUE WHERE BATCH_ID = " + BATCH_ID + "" } );

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
	