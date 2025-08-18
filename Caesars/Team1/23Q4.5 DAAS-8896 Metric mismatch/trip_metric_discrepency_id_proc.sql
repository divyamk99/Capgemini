CREATE OR REPLACE PROCEDURE DAAS_ADHOC.TRIP_METRIC_DISCREPENCY_ID_PROC()
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
	

	snowflake.execute( {sqlText: `CREATE OR REPLACE TABLE DAAS_TEMP.METRIC_GUID AS
	SELECT DISTINCT GUEST_UNIQUE_ID FROM DAAS_TEMP.RATINGS_DISCREPANCY_GUEST_IDS
	UNION
	SELECT DISTINCT GUEST_UNIQUE_ID FROM DAAS_TEMP.POS_DISCREPANCY_GUEST_IDS
	UNION
	SELECT DISTINCT GUEST_UNIQUE_ID FROM DAAS_TEMP.HOTEL_DISCREPANCY_GUEST_IDS
	UNION
	SELECT DISTINCT GUEST_UNIQUE_ID FROM DAAS_TEMP.OFFER_DISCREPANCY_GUEST_IDS
	UNION
	SELECT DISTINCT GUEST_UNIQUE_ID FROM DAAS_TEMP.COMP_DISCREPANCY_GUEST_IDS;
	` } );
		
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