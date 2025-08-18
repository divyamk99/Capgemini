CREATE OR REPLACE PROCEDURE DAAS_ADHOC.offer_purge_create_temp_adhoc_proc_8396()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT 
EXECUTE AS OWNER AS $$
proc_output = "";
proc_step = "";

try
{
	snowflake.execute( {sqlText: "BEGIN;" } );
	snowflake.execute( {sqlText:`CREATE OR REPLACE TABLE DAAS_TEMP.OFFER_FIX AS SELECT DISTINCT TRANSACTION_TABLE_SK FROM DAAS_CORE.TRIP_DETAIL WHERE TRANSACTION_TABLE_SK IN (  SELECT DISTINCT OFFER_LEDGER_FACT_SK   FROM DAAS_CORE.OFFER_LEDGER_FACT_HISTORY   WHERE   OFFER_STATUS_CD IN ('R','X')  AND CURRENT_STATUS_IND = 'N' AND DELETE_IND = 'Y' ) AND TRANSACTION_TYPE = 'OFFER' AND DELETE_IND = 'N';` } );
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
return proc_output ;
$$ ;