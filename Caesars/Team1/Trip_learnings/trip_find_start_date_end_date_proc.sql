CREATE OR REPLACE PROCEDURE DAAS_COMMON.TRIP_FIND_START_DATE_END_DATE_PROC(BATCH_ID FLOAT, SHARD_NAME VARCHAR, SOURCE_TABLE_NAME VARCHAR, TARGET_TABLE_NAME VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS 
$$

/*
#####################################################################################
Author: Sruthi Thammana 
Purpose: Identifies Trip Start Date and Trip end date for each transaction and loads into TARGET_TABLE_NAME
Input Parameters: Source table that contains unmapped transactions
Output Value: SUCCESS/FAILURE
Create Date: 06/23/2022
Modify Date: 
Version: 1.0
Modified By: Nambi
Version: 1.1 (Remove CREATE or REPLACE and use INSERT INTO)
Modified By: Sruthi Thammana
Modified Date : 07/25/2022
Version: 1.2 (Changes to incorporate Hotel transactions)
Modified By : Sunita V
Modified Date: 06/09/2023
Version: 1.3
Purpose: To add property_cd in trip_detail table(search with text DETAIL_PROPERTY_CD for changes)
#####################################################################################
*/
try
{
	var proc_step = 'load unmapped records into target table';
	tag = BATCH_ID + "_TRIP_FIND_START_DATE_END_DATE_PROC";
	
	var my_sql_command_A = `SELECT VALUE FROM DAAS_CORE.TRIP_CONFIG WHERE CONFIG_SUBTYPE='ENTERPRISE' AND KEY='MAX_TRIP_DAYS';`;
    var statementA = snowflake.createStatement({sqlText: my_sql_command_A});
    ent_max_trip = statementA.execute();
    ent_max_trip.next();
    get_ent_max_trip = ent_max_trip.getColumnValue(1);
	
	var my_sql_command_B = `SELECT VALUE FROM DAAS_CORE.TRIP_CONFIG WHERE CONFIG_SUBTYPE='ENTERPRISE' AND KEY='TRIP_GAP_DAYS';`;
    var statementB = snowflake.createStatement({sqlText: my_sql_command_B});
	ent_trip_gap = statementB.execute();
    ent_trip_gap.next();
    get_ent_trip_gap = ent_trip_gap.getColumnValue(1); 

    hotel_max_trip = snowflake.execute({sqlText: "SELECT VALUE FROM DAAS_CORE.TRIP_CONFIG WHERE CONFIG_SUBTYPE = 'HOTEL' AND KEY = 'MAX_TRIP_DAYS'"});
	hotel_max_trip.next();
	hotel_max_trip_days = hotel_max_trip.getColumnValue(1);	

	/*DELETE records from DAAS_TEMP.TRIP_ROW_GEN*/

	my_sql_command_0 = `TRUNCATE TABLE DAAS_TEMP.TRIP_ROW_GEN`;	
	var proc_output = my_sql_command_0;
	snowflake.execute( {sqlText: my_sql_command_0});


	/*Insert records into DAAS_TEMP.TRIP_ROW_GEN */

	my_sql_command_0a = `INSERT INTO DAAS_TEMP.TRIP_ROW_GEN SELECT SEQ4() AS NUMBER FROM TABLE(generator(rowcount => 1000))`;	
	var proc_output=my_sql_command_0a;
	snowflake.execute( {sqlText: my_sql_command_0a});

	/*DELETE records from Target Table*/

	my_sql_command_0b = `TRUNCATE TABLE `+TARGET_TABLE_NAME+``;	
	var proc_output=my_sql_command_0b;
	snowflake.execute( {sqlText: my_sql_command_0b});
	
	my_sql_command_0c = `TRUNCATE TABLE DAAS_TEMP.TRIP_UNMAPPED_SPLIT_TRANS_TEMP`;	
	var proc_output=my_sql_command_0c;
	snowflake.execute( {sqlText: my_sql_command_0c});

	/*Copy all the transactions with Trip Identifier as N from SOURCE_TABLE_NAME to TARGET_TABLE_NAME*/

	my_sql_command_1 = `INSERT INTO DAAS_TEMP.TRIP_UNMAPPED_SPLIT_TRANS_TEMP
	SELECT DISTINCT
		GUEST_UNIQUE_ID,
		PROPERTY_CD,
		DETAIL_PROPERTY_CD,
		TRIP_TYPE,
		MARKET_CD,
		TRANSACTION_TABLE_SK,
		TRANSACTION_TABLE,
		TRANSACTION_TYPE,
		TRANSACTION_START_DTTM,
		TRANSACTION_END_DTTM,
		BUSINESS_START_DT + NUMBER AS BUSINESS_START_DT,
		BUSINESS_START_DT + NUMBER AS BUSINESS_END_DT,
		TRANSACTION_MODIFIED_DTTM,
		TRIP_INDICATOR,
		'UNMAPPED' AS MAPPING_STATUS,
		NULL AS TRIP_START_DT,
		NULL AS TRIP_END_DT 
	FROM 
		`+SOURCE_TABLE_NAME+` A
		JOIN
		DAAS_TEMP.TRIP_ROW_GEN B
		ON
		(DATEDIFF(DAY, A.BUSINESS_START_DT, A.BUSINESS_END_DT) + 1) > B.NUMBER 
	WHERE
		 TRANSACTION_TYPE = 'HOTEL'
		 AND TRIP_INDICATOR = 'N'
	UNION ALL
	SELECT 
		GUEST_UNIQUE_ID,
		PROPERTY_CD,
		DETAIL_PROPERTY_CD,
		TRIP_TYPE,
		MARKET_CD,
		TRANSACTION_TABLE_SK,
		TRANSACTION_TABLE,
		TRANSACTION_TYPE,
		TRANSACTION_START_DTTM,
		TRANSACTION_END_DTTM,
		BUSINESS_START_DT,
		BUSINESS_END_DT,
		TRANSACTION_MODIFIED_DTTM,
		TRIP_INDICATOR,
		'UNMAPPED' AS MAPPING_STATUS,
		NULL AS TRIP_START_DT,
		NULL AS TRIP_END_DT 
	FROM 
		`+SOURCE_TABLE_NAME+` 
	WHERE
		TRANSACTION_TYPE <> 'HOTEL'
		AND TRIP_INDICATOR = 'N'
	`;	

	var proc_output=my_sql_command_1;
	snowflake.execute( {sqlText: my_sql_command_1});

	/*Get count of unmapped records*/
	proc_step='Get count of unmapped records';
	var my_sql_command_1a = `SELECT COUNT(*) FROM DAAS_TEMP.TRIP_UNMAPPED_SPLIT_TRANS_TEMP WHERE MAPPING_STATUS='UNMAPPED'`;
	var statement1a = snowflake.createStatement({sqlText: my_sql_command_1a});
	proc_output=my_sql_command_1a;
	var count_unmapped = statement1a.execute();
	count_unmapped.next();
	var get_count_unmapped = count_unmapped.getColumnValue(1);

	/*Loop to determine trip_start_dt and trip_end_dt for each transactions*/
	proc_step='Loop to determine trip_start_dt and trip_end_dt for each transactions';
	while(get_count_unmapped>0)
	{
		/*Delete existing records from DAAS_TEMP.TRIP_UNMAPPED_TRANS_HOTEL_TEMP*/
		my_sql_command_1b = `TRUNCATE TABLE DAAS_TEMP.TRIP_UNMAPPED_TRANS_HOTEL_TEMP`;	
		var proc_output=my_sql_command_1b;
		snowflake.execute( {sqlText: my_sql_command_1b});

		/*Copy all the transactions  TARGET_TABLE_NAME to DAAS_TEMP.TRIP_UNMAPPED_TRANS_HOTEL_TEMP*/
		my_sql_command_1c = `
		INSERT INTO DAAS_TEMP.TRIP_UNMAPPED_TRANS_HOTEL_TEMP
		SELECT 
			GUEST_UNIQUE_ID,
			PROPERTY_CD,
			DETAIL_PROPERTY_CD,
			TRIP_TYPE,
			MARKET_CD,
			TRANSACTION_TABLE_SK,
			TRANSACTION_TABLE,
			TRANSACTION_START_DTTM,
			TRANSACTION_END_DTTM,
			BUSINESS_START_DT,
			BUSINESS_END_DT,
			TRANSACTION_MODIFIED_DTTM,
			TRIP_INDICATOR,
			MAPPING_STATUS,
			TRIP_START_DT,
			TRIP_END_DT
		FROM 
			DAAS_TEMP.TRIP_UNMAPPED_SPLIT_TRANS_TEMP
		WHERE 
			MAPPING_STATUS = 'UNMAPPED' 
			AND TRANSACTION_TABLE = 'HOTEL_GUEST_RESERVATIONS_FACT' 
		`;	

		var proc_output=my_sql_command_1c;
		snowflake.execute( {sqlText: my_sql_command_1c});

		my_sql_command_2 = `
		UPDATE DAAS_TEMP.TRIP_UNMAPPED_SPLIT_TRANS_TEMP AS TGT 
		SET 
			TGT.MAPPING_STATUS = SRC.MAPPING_STATUS,
			TGT.TRIP_START_DT = SRC.TRIP_START_DT,
			TGT.TRIP_END_DT = SRC.TRIP_END_DT
		FROM 
		( 
			SELECT 
				GUEST_UNIQUE_ID,
				PROPERTY_CD,
				TRIP_TYPE,
				MARKET_CD,
				TRANSACTION_TABLE_SK,
				TRANSACTION_TABLE,
				BUSINESS_START_DT,
				BUSINESS_END_DT,
				MAPPING_STATUS_NEW AS MAPPING_STATUS,
				TRIP_START_DT,
				MAX(CASE WHEN MAPPING_STATUS_NEW = 'MAPPED' THEN BUSINESS_END_DT END) OVER (PARTITION BY GROUPNUMBER) AS TRIP_END_DT 
			FROM
			(
				SELECT 
					*,
					MAX(A.MAPPING_STATUS) OVER (PARTITION BY GROUPNUMBER 
						ORDER BY 
								A.GUEST_UNIQUE_ID, 
								A.PROPERTY_CD, 
								A.TRIP_TYPE, 
								A.MARKET_CD,
								A.BUSINESS_START_DT,
								A.BUSINESS_END_DT ,
								--A.TRANSACTION_TABLE_SK,
								A.TRANSACTION_TABLE_NEW  ROWS UNBOUNDED PRECEDING 
					)  AS MAPPING_STATUS_NEW
				FROM 
				(
					SELECT 
						A.GUEST_UNIQUE_ID,
						A.PROPERTY_CD,
						A.TRIP_TYPE,
						A.MARKET_CD,
						A.TRANSACTION_TABLE_SK,
						A.TRANSACTION_TABLE,
						A.BUSINESS_START_DT,
						A.BUSINESS_END_DT,
						GROUPNUMBER,
						CASE WHEN A.TRANSACTION_TABLE='HOTEL_GUEST_RESERVATIONS_FACT' THEN 'A' ELSE A.TRANSACTION_TABLE END AS TRANSACTION_TABLE_NEW,
						DURATION,
						CASE WHEN B.GUEST_UNIQUE_ID IS NOT NULL THEN 'MAPPED' ELSE A.MAPPING_STATUS END AS MAPPING_STATUS,
						A.TRIP_START_DT
					FROM
					(
						SELECT 
							*,
							COALESCE(MIN(TRIP_START_DATE_TEMP_1) OVER (PARTITION BY GROUPNUMBER), BUSINESS_START_DT) AS TRIP_START_DT,
							DATEDIFF(DAY, TRIP_START_DT,BUSINESS_START_DT) + 1 AS DURATION,
							CASE 
								WHEN 
									TRANSACTION_TABLE='HOTEL_GUEST_RESERVATIONS_FACT' AND DURATION <= ` + hotel_max_trip_days + `
									OR (TRIP_TYPE = 'PROPERTY' AND DURATION <= DAAS_COMMON.TRIP_CONFIG_UDF_PROPERTY('MAX_TRIP_DAYS', PROPERTY_CD, BUSINESS_START_DT)) 
									OR (TRIP_TYPE = 'MARKET' AND DURATION <= DAAS_COMMON.TRIP_CONFIG_UDF_MARKET('MAX_TRIP_DAYS', MARKET_CD, BUSINESS_START_DT))  
									OR (TRIP_TYPE = 'ENTERPRISE' AND DURATION <= `+get_ent_max_trip+`)
								THEN 
									'MAPPED'
								ELSE 
									'UNMAPPED' 
								END 
							AS MAPPING_STATUS 
						FROM
						(
							SELECT 
								*,
								COALESCE(TRIP_START_DATE_TEMP, BUSINESS_START_DT) AS TRIP_START_DATE_TEMP_1,
								SUM(BOUNDARY) OVER(ORDER BY 
									GUEST_UNIQUE_ID,
									PROPERTY_CD,
									TRIP_TYPE,
									MARKET_CD,
									BUSINESS_START_DT,
									BUSINESS_END_DT,
									TRANSACTION_TABLE_SK,
									TRANSACTION_TABLE DESC ROWS UNBOUNDED PRECEDING
								)  AS GROUPNUMBER
							FROM
							(
								SELECT 
									A.GUEST_UNIQUE_ID,
									A.PROPERTY_CD,
									A.TRIP_TYPE,
									A.MARKET_CD,
									A.TRANSACTION_TABLE_SK,
									A.TRANSACTION_TABLE,
									A.BUSINESS_START_DT,
									A.BUSINESS_END_DT,
									LAG(A.BUSINESS_START_DT,1) OVER (PARTITION BY A.GUEST_UNIQUE_ID,
										A.PROPERTY_CD,
										A.TRIP_TYPE,
										A.MARKET_CD 
										ORDER BY A.BUSINESS_START_DT,A.BUSINESS_END_DT,A.TRANSACTION_TABLE_SK,A.TRANSACTION_TABLE DESC) 
									AS PREVIOUS_BUSINESS_START_DATE,
									CASE 
										WHEN 
											(A.TRIP_TYPE = 'PROPERTY' AND DATEDIFF(DAY, PREVIOUS_BUSINESS_START_DATE, A.BUSINESS_START_DT) BETWEEN 0 AND DAAS_COMMON.TRIP_CONFIG_UDF_PROPERTY('TRIP_GAP_DAYS', A.PROPERTY_CD, A.BUSINESS_START_DT))
											OR (A.TRIP_TYPE = 'MARKET' AND DATEDIFF(DAY, PREVIOUS_BUSINESS_START_DATE, A.BUSINESS_START_DT) BETWEEN 0 AND DAAS_COMMON.TRIP_CONFIG_UDF_MARKET('TRIP_GAP_DAYS', A.MARKET_CD, A.BUSINESS_START_DT))
											OR (A.TRIP_TYPE = 'ENTERPRISE' AND DATEDIFF(DAY, PREVIOUS_BUSINESS_START_DATE, A.BUSINESS_START_DT) BETWEEN 0 AND `+ get_ent_trip_gap +`)
										THEN PREVIOUS_BUSINESS_START_DATE 
									END AS TRIP_START_DATE_TEMP,
									CASE WHEN TRIP_START_DATE_TEMP IS NULL THEN 1 ELSE 0 END AS BOUNDARY
								FROM 
								(
									SELECT 
										GUEST_UNIQUE_ID,
										PROPERTY_CD,
										TRIP_TYPE,
										MARKET_CD,
										TRANSACTION_TABLE_SK,
										TRANSACTION_TABLE,
										TRANSACTION_START_DTTM,
										TRANSACTION_END_DTTM,
										BUSINESS_START_DT,
										BUSINESS_END_DT,
										TRANSACTION_MODIFIED_DTTM,
										MAPPING_STATUS,
										TRIP_START_DT,
										TRIP_END_DT
									FROM 
										DAAS_TEMP.TRIP_UNMAPPED_SPLIT_TRANS_TEMP A
									WHERE
										A.MAPPING_STATUS = 'UNMAPPED'	
								) A
							)
							--QUALIFY ROW_NUMBER() OVER (PARTITION BY GUEST_UNIQUE_ID, PROPERTY_CD, TRIP_TYPE, MARKET_CD, TRANSACTION_TABLE_SK, TRANSACTION_TABLE ORDER BY BUSINESS_START_DT, BUSINESS_END_DT) = 1
						)
					) A
					LEFT JOIN
						DAAS_TEMP.TRIP_UNMAPPED_TRANS_HOTEL_TEMP B
					ON
						A.GUEST_UNIQUE_ID = B.GUEST_UNIQUE_ID
						AND A.PROPERTY_CD = B.PROPERTY_CD
						AND A.TRIP_TYPE = B.TRIP_TYPE
						AND A.MARKET_CD = B.MARKET_CD
						AND A.BUSINESS_START_DT BETWEEN B.BUSINESS_START_DT AND B.BUSINESS_END_DT
						AND A.MAPPING_STATUS = 'UNMAPPED'
						AND A.TRANSACTION_TABLE <> 'HOTEL_GUEST_RESERVATIONS_FACT'
						
				)A 
			)
			--QUALIFY ROW_NUMBER() OVER (PARTITION BY GUEST_UNIQUE_ID, PROPERTY_CD, TRIP_TYPE, MARKET_CD, TRANSACTION_TABLE_SK, TRANSACTION_TABLE 
			--ORDER BY BUSINESS_START_DT, BUSINESS_END_DT) = 1
				ORDER BY GUEST_UNIQUE_ID,BUSINESS_START_DT,BUSINESS_END_DT
		) SRC
		WHERE 
			SRC.MAPPING_STATUS = 'MAPPED' 
			AND TGT.TRANSACTION_TABLE_SK = SRC.TRANSACTION_TABLE_SK
			AND TGT.TRANSACTION_TABLE = SRC.TRANSACTION_TABLE
			AND TGT.GUEST_UNIQUE_ID = SRC.GUEST_UNIQUE_ID
			AND TGT.TRIP_TYPE = SRC.TRIP_TYPE
			AND TGT.PROPERTY_CD = SRC.PROPERTY_CD
			AND TGT.MARKET_CD = SRC.MARKET_CD 
			AND TGT.BUSINESS_START_DT = SRC.BUSINESS_START_DT
			AND TGT.BUSINESS_END_DT = SRC.BUSINESS_END_DT
		`;	
		proc_output=my_sql_command_2;
		snowflake.execute( {sqlText: my_sql_command_2});

		var my_sql_command_3 = `SELECT COUNT(*) FROM DAAS_TEMP.TRIP_UNMAPPED_SPLIT_TRANS_TEMP WHERE MAPPING_STATUS='UNMAPPED'`;
		var statement0 = snowflake.createStatement({sqlText: my_sql_command_3});
		count_unmapped = statement0.execute();
		count_unmapped.next();
		get_count_unmapped = count_unmapped.getColumnValue(1);
	}
	/*Copy all the transactions with Trip Identifier as N from SOURCE_TABLE_NAME to TARGET_TABLE_NAME*/

	my_sql_command_20 = `INSERT INTO `+TARGET_TABLE_NAME+`
	SELECT DISTINCT
		GUEST_UNIQUE_ID,
		PROPERTY_CD,
		DETAIL_PROPERTY_CD,
		TRIP_TYPE,
		MARKET_CD,
		TRANSACTION_TABLE_SK,
		TRANSACTION_TABLE,
		TRANSACTION_TYPE,
		TRANSACTION_START_DTTM,
		TRANSACTION_END_DTTM,
		MIN(BUSINESS_START_DT) AS BUSINESS_START_DT,
		MAX(BUSINESS_END_DT) AS BUSINESS_END_DT,
		TRANSACTION_MODIFIED_DTTM,
		TRIP_INDICATOR,
		MAPPING_STATUS,
		TRIP_START_DT,
		TRIP_END_DT 
	FROM 
		DAAS_TEMP.TRIP_UNMAPPED_SPLIT_TRANS_TEMP A
		GROUP BY
		GUEST_UNIQUE_ID,
		PROPERTY_CD,
		DETAIL_PROPERTY_CD,
		TRIP_TYPE,
		MARKET_CD,
		TRANSACTION_TABLE_SK,
		TRANSACTION_TABLE,
		TRANSACTION_TYPE,
		TRANSACTION_START_DTTM,
		TRANSACTION_END_DTTM,
		TRANSACTION_MODIFIED_DTTM,
		TRIP_INDICATOR,
		MAPPING_STATUS,
		TRIP_START_DT,
		TRIP_END_DT`;
		
		var proc_output=my_sql_command_20;
	snowflake.execute( {sqlText: my_sql_command_20});
	
	proc_output = "SUCCESS";
}
catch (err) 
{ 
	proc_output = "FAILURE";
	error_code = "Failed: Code: " + err.code + "  State: " + err.state;
	error_message = "\n  Message: " + err.message + "\nStack Trace:\n" + err.stackTraceTxt;
	error_message = error_message.replace(/["'"]/g, "");
}
return proc_output;
$$