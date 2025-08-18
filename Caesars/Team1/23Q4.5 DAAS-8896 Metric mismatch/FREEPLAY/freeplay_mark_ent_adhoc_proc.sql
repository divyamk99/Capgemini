CREATE OR REPLACE PROCEDURE DAAS_ADHOC.FREEPLAY_MARK_ENT_ADHOC_PROC()
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
	

	snowflake.execute( {sqlText: `CREATE OR REPLACE TABLE DAAS_TEMP.FREEPLAY_MARK_ENT_DESCRIPANCY
									AS
									WITH FREEPLAY AS
									(
									WITH ABS_LOGIC AS
									(
									SELECT
										TD.TRIP_MASTER_ID,
										COALESCE(SUM(CASE WHEN TD.DELETE_IND = 'N' THEN FREE_PLAY_POINTS END),0) AS DLC_FSP_POINT_AMT_LOGIC,
										CASE WHEN DLC_FSP_POINT_AMT_LOGIC <0 THEN -1 ELSE 1 END SIGN
									FROM
										DAAS_CORE.TRIP_DETAIL TD
									JOIN 
										DAAS_CORE.TRIP_MASTER TM 
									ON
										TD.TRIP_MASTER_ID = TM.TRIP_MASTER_ID
									JOIN
										DAAS_CORE.COMP_FREE_PLAY_DETAIL_FACT CFPDF
									ON
										TD.TRANSACTION_TABLE_SK = CFPDF.COMP_FREE_PLAY_DETAIL_FACT_SK
									WHERE
										TD.TRANSACTION_TYPE = 'FREEPLAY'
									AND 
										TM.DELETE_IND = 'N'
									GROUP BY
										TD.TRIP_MASTER_ID
									)
									SELECT
										DISTINCT GUEST_UNIQUE_ID,
										ABS(SUM(CASE WHEN TRIP_TYPE = 'MARKET' AND MARKET_CD <> 'CH2' THEN TS.DLC_FSP_POINT_AMT * AL.SIGN ELSE 0 END)) AS MARKET_DLC_FSP_POINT_AMT,
										ABS(SUM(CASE WHEN TRIP_TYPE = 'ENTERPRISE' THEN TS.DLC_FSP_POINT_AMT * AL.SIGN ELSE 0 END)) AS ENTERPRISE_DLC_FSP_POINT_AMT,
										ABS(SUM(CASE WHEN TRIP_TYPE = 'MARKET' AND MARKET_CD <> 'CH2' THEN TS.DLC_FSP_DOLLAR_AMT * AL.SIGN ELSE 0 END)) AS MARKET_DLC_FSP_DOLLAR_AMT,
										ABS(SUM(CASE WHEN TRIP_TYPE = 'ENTERPRISE' THEN TS.DLC_FSP_DOLLAR_AMT * AL.SIGN ELSE 0 END)) AS ENTERPRISE_DLC_FSP_DOLLAR_AMT
									FROM 
										DAAS_CORE_MARKETING_VW.TRIP_SUMMARY_VW TS
									JOIN 
										ABS_LOGIC AL
									ON 
										TS.TRIP_MASTER_ID = AL.TRIP_MASTER_ID 
									AND 
										DLC_FSP_FLG = 'Y'
									AND TS.GUEST_UNIQUE_ID <> -1
									GROUP BY
										GUEST_UNIQUE_ID
									)
									SELECT DISTINCT GUEST_UNIQUE_ID
									FROM FREEPLAY
									WHERE MARKET_DLC_FSP_POINT_AMT <> ENTERPRISE_DLC_FSP_POINT_AMT
									OR MARKET_DLC_FSP_DOLLAR_AMT <> ENTERPRISE_DLC_FSP_DOLLAR_AMT
									;` } );
				
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