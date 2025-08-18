CREATE OR REPLACE PROCEDURE DAAS_ADHOC.RATING_CLEANUP_ADHOC_PROC(LOWER_LIMIT INT,UPPER_LIMIT INT)
RETURNS VARCHAR NOT NULL
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$

proc_output = "";
proc_step = "";

try
{
	snowflake.execute( {sqlText: "BEGIN;" } );
	

	update_query = snowflake.execute( {sqlText: `UPDATE DAAS_CORE.TRIP_DETAIL
	SET DELETE_IND = 'Y',
	TRANSACTION_SUB_TYPE = 'VOIDED'
	WHERE TRANSACTION_TABLE_SK IN (SELECT DISTINCT TRANSACTION_TABLE_SK FROM DAAS_TEMP.RATING_EXTRA_SKS_CLEANUP_TRIPS 
	WHERE RANK BETWEEN `+ LOWER_LIMIT +` AND `+ UPPER_LIMIT +`)
	AND TRANSACTION_TYPE = 'RATINGS';` } );
	update_query.next();
    update_result=update_query.getColumnValue(1);
	

	insert_query = snowflake.execute( {sqlText: `INSERT INTO DAAS_CORE.TRIP_RECALC_QUEUE
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
		TM.TRIP_MASTER_ID,
		'PENDING' AS STATUS,
		20240105,
		CURRENT_TIMESTAMP AS CREATED_DTTM,
		CURRENT_USER AS CREATED_BY,
		CURRENT_TIMESTAMP AS UPDATED_DTTM,
		CURRENT_USER AS UPDATED_BY
	FROM
		DAAS_CORE.TRIP_MASTER TM
	WHERE 
		TM.TRIP_MASTER_ID IN (SELECT DISTINCT TRIP_MASTER_ID FROM DAAS_TEMP.RATING_EXTRA_SKS_CLEANUP_TRIPS 
		WHERE RANK BETWEEN `+ LOWER_LIMIT +` AND `+ UPPER_LIMIT +` )
	AND TM.DELETE_IND = 'N';` } );
	
	insert_query.next();
    insert_result=insert_query.getColumnValue(1);
	
	snowflake.execute( {sqlText: "COMMIT;" } );

	proc_output += "Update" + update_result;
	proc_output += "Insert" + insert_result;
}	

catch (err) 
{ 
snowflake.execute( {sqlText: "ROLLBACK;" } );

proc_output += "\n Failed: Code: " + err.code + "\n  State: " + err.state;
proc_output += "\n  Message: " + err.message;
proc_output += "\nStack Trace:\n" + err.stackTraceTxt;
}
 
return proc_output;
$$;