CREATE OR REPLACE PROCEDURE DAAS_ADHOC.TRIP_METRIC_REVERT_20231226()
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
	
	snowflake.execute( {sqlText: "TRUNCATE TABLE DAAS_CORE.TRIP_DETAIL;"} );
	snowflake.execute( {sqlText: "TRUNCATE TABLE DAAS_CORE.TRIP_MASTER;"} );
	snowflake.execute( {sqlText: "TRUNCATE TABLE DAAS_CORE.TRIP_SUMMARY;"} );
	snowflake.execute( {sqlText: "TRUNCATE TABLE DAAS_CORE.DAILY_ACTIVITY_SUMMARY;"} );
	snowflake.execute( {sqlText: "TRUNCATE TABLE DAAS_CORE.TRIP_MERGE_PROCESSED; "} );
	snowflake.execute( {sqlText: "TRUNCATE TABLE DAAS_CORE.TRIP_MERGE_QUEUE;"} );
	snowflake.execute( {sqlText: "TRUNCATE TABLE DAAS_CORE.TRIP_RECALC_PROCESSED;"} );
	snowflake.execute( {sqlText: "TRUNCATE TABLE DAAS_CORE.TRIP_RECALC_QUEUE;"} );

	snowflake.execute( {sqlText: "INSERT INTO DAAS_CORE.TRIP_DETAIL SELECT * FROM DAAS_TEMP.TRIP_DETAIL_BKP_20231226;"} );
	snowflake.execute( {sqlText: "INSERT INTO DAAS_CORE.TRIP_MASTER SELECT * FROM DAAS_TEMP.TRIP_MASTER_BKP_20231226;"} );
	snowflake.execute( {sqlText: "INSERT INTO DAAS_CORE.TRIP_SUMMARY SELECT * FROM DAAS_TEMP.TRIP_SUMMARY_BKP_20231226;"} );
	snowflake.execute( {sqlText: "INSERT INTO DAAS_CORE.DAILY_ACTIVITY_SUMMARY SELECT * FROM DAAS_TEMP.DAILY_ACTIVITY_SUMMARY_BKP_20231226;"} );
	snowflake.execute( {sqlText: "INSERT INTO DAAS_CORE.TRIP_MERGE_PROCESSED SELECT * FROM DAAS_TEMP.TRIP_MERGE_PROCESSED_BKP_20231226;"} );
	snowflake.execute( {sqlText: "INSERT INTO DAAS_CORE.TRIP_MERGE_QUEUE SELECT * FROM DAAS_TEMP.TRIP_MERGE_QUEUE_BKP_20231226;"} );
	snowflake.execute( {sqlText: "INSERT INTO DAAS_CORE.TRIP_RECALC_PROCESSED SELECT * FROM DAAS_TEMP.TRIP_RECALC_PROCESSED_BKP_20231226;"} );
	snowflake.execute( {sqlText: "INSERT INTO DAAS_CORE.TRIP_RECALC_QUEUE SELECT * FROM DAAS_TEMP.TRIP_RECALC_QUEUE_BKP_20231226;"} );
	
		
	snowflake.execute( {sqlText: "COMMIT;" } );

	proc_output = "SUCCESS";
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







