CREATE OR REPLACE PROCEDURE DAAS_ADHOC.RATING_EXTRA_SK_ADHOC_PROC()
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
	

	snowflake.execute( {sqlText: `CREATE OR REPLACE TABLE DAAS_TEMP.RATING_EXTRA_SKS_CLEANUP_TRIPS AS
								SELECT DISTINCT TM.TRIP_MASTER_ID,TD.TRANSACTION_TABLE_SK,DENSE_RANK() OVER(ORDER BY TM.TRIP_MASTER_ID) AS RANK
								FROM DAAS_CORE.TRIP_MASTER TM
								JOIN DAAS_CORE.TRIP_DETAIL TD
								ON TM.TRIP_MASTER_ID = TD.TRIP_MASTER_ID
								WHERE TM.DELETE_IND = 'N'
								AND TD.DELETE_IND = 'N'
								AND TD.TRANSACTION_TYPE = 'RATINGS'
								AND TM.GUEST_UNIQUE_ID <> -1
								AND TRANSACTION_TABLE_SK NOT IN (SELECT RATINGS_FACT_SK FROM DAAS_CORE.RATINGS_FACT;` } );
	
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