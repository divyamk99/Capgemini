CREATE OR REPLACE PROCEDURE DAAS_COMMON.CD_ET_BOUNCES_EVENT_HISTORY_EXTRACT_PROC()
RETURNS VARCHAR NOT NULL 
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$ 
/*
##########################################################################################
Author: Divya
Purpose: Load historical data from S3 to Stage (DAAS_STG_EXACT_TARGET.CD_ET_BOUNCES_EVENT_STG) 
##########################################################################################
*/

proc_output = "";

try
{ 
	snowflake.execute( {sqlText: "BEGIN;" } );

	/* ET_BOUNCES_EVENT */

	var my_sql_command_1 = `
	COPY INTO DAAS_STG_EXACT_TARGET.CD_ET_BOUNCES_EVENT_STG
	(
		I_CLIENTID,                
		I_SENDID,                  
		C_SUBSCRIBER_KEY,          
		C_EMAILADDRESS,            
		I_SUBSCRIBERID,            
		I_LISTID,                 
		D_EVENTDATE,               
		C_EVENTTYPE,               
		C_BOUNCE_CATEGORY,         
		I_SMTP_CODE,               
		C_BOUNCE_REASON,           
		C_BATCHID,                 
		C_TRIGGEREDSENDEXTERNALKEY,
		SOURCE_SYSTEM_NAME,        
		TIME_ZONE, 				  
		CREATED_DATE, 			  
		CREATED_BY,				  
		UPDATED_DATE,			  
		UPDATED_BY, 				  
		REPLAY_COUNTER, 			  
		SOURCE_FILE_NAME
	)
	FROM
	(SELECT 
		TRIM($1),
		TRIM($2),
		TRIM($3),
		TRIM($4),
		TRIM($5),
		TRIM($6),
		TRIM($7),
		TRIM($8),
		TRIM($9),
		TRIM($10),
		TRIM($11),
		TRIM($12),
		TRIM($13),
		'EXACT TARGET',
		'US/CENTRAL',
		CURRENT_TIMESTAMP,
		CURRENT_USER,
		CURRENT_TIMESTAMP,
		CURRENT_USER,
		0,
		METADATA$FILENAME
	FROM @DAAS_COMMON.DAAS_EXT_STG_HOSTED/exact-target/out/CD_ET
	)
	PATTERN = '.*CD_Bounces.*'
	/*FILE_FORMAT = 'DAAS_COMMON.ET_PIPE_FORMAT'*/
	file_format = (type = csv field_delimiter = '|' skip_header = 1 escape = '\134' encoding = 'iso-8859-1') ;
	`;

	var statement_1 = snowflake.createStatement( {sqlText: my_sql_command_1 } );
	proc_output = my_sql_command_1; 
	var proc_status1 = statement_1.execute();


	snowflake.execute( {sqlText: "COMMIT;" } );	
	proc_output = "SUCCESS";
}

catch (err) 
{ 
	snowflake.execute( {sqlText: "ROLLBACK;" } );

	proc_output += "\n Failed: Code: " + err.code + "\n  State: " + err.state;
	proc_output += "\n  Message: " + err.message;
	proc_output += "\n Stack Trace:\n" + err.stackTraceTxt;
}
return proc_output;
$$;