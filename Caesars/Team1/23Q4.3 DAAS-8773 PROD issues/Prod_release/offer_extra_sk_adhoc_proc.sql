CREATE OR REPLACE PROCEDURE DAAS_ADHOC.OFFER_EXTRA_SK_ADHOC_PROC()
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
	

	snowflake.execute( {sqlText: `CREATE OR REPLACE TABLE DAAS_TEMP.OFFERS_EXTRA_SKS_CLEANUP_TRIPS AS
								SELECT DISTINCT TM.TRIP_MASTER_ID, TD.TRANSACTION_TABLE_SK ,
								DENSE_RANK() OVER(ORDER BY TM.TRIP_MASTER_ID) AS RANK
								FROM DAAS_CORE.TRIP_DETAIL TD
								OIN DAAS_CORE.TRIP_MASTER TM
								ON TD.TRIP_MASTER_ID = TM.TRIP_MASTER_ID
								WHERE TRANSACTION_TABLE_SK NOT IN (SELECT DISTINCT OFFER_LEDGER_FACT_SK FROM DAAS_CORE_MARKETING_VW.OFFER_LEDGER_FACT_TRIP_VW)
								AND TD.TRANSACTION_TYPE = 'OFFER'
								AND TM.DELETE_IND = 'N'
								AND TD.DELETE_IND = 'N'
								AND TM.GUEST_UNIQUE_ID <> -1;` } );
	
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