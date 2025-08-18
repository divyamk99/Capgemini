CREATE OR REPLACE PROCEDURE DAAS_COMMON.TRIP_HOTEL_DOMAIN_PIPELINE_PROC(BATCH_ID FLOAT, SHARD_NAME VARCHAR, WAREHOUSE_NAME VARCHAR, CUSTOM_PARAM1 VARCHAR, CUSTOM_PARAM2 VARCHAR) 
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER AS
$$			
			
		
/*
#####################################################################################
Author: HEMANTH V 
Purpose:  
Input Parameters: BATCH_ID, SHARD_NAME, WAREHOUSE_NAME, CUSTOM_PARAM1, CUSTOM_PARAM2
Output Value: SUCCESS for successful execution and for failed execution it returns sql statement causing issue with error description
Create Date:  
Version: 1.0
MODIFY : HEMANTH V 
Version: 1.4
Purpose: Change in checkout date causing  duplicate issue in hotel domain  pipeline (to address it we have added a few lines of code)
1) commented one filter in statement my_sql_command_4   where we update matching SK to trip_detail
(--AND FTMP.CHECK_IN_DTTM::DATE=TD.TRANSACTION_START_DTTM::DATE  AND TD.TRANSACTION_END_DTTM::DATE=FTMP.CHECK_OUT_DTTM::DATE) 
2) added TRANSACTION_END_DTTM,BUSINESS_END_DT for update statement in  my_sql_command_4
    
	TD.TRANSACTION_END_DTTM=IFF(TD.TRANSACTION_END_DTTM ::DATE<> FTMP.CHECK_OUT_DTTM::DATE,FTMP.CHECK_OUT_DTTM::DATE,TD.TRANSACTION_END_DTTM::DATE),
	TD.BUSINESS_END_DT=IFF(TD.BUSINESS_END_DT ::DATE<> FTMP.CHECK_OUT_DTTM::DATE,FTMP.CHECK_OUT_DTTM::DATE,TD.BUSINESS_END_DT::DATE) 
3) Moved my_sql_command_4_0 position before SK getting updated ( statement my_sql_command_4)(this will allow such  record to iteration 3)

MODIFY : HEMANTH V 
Version: 1.2	
FIX : ISSUE (DAASS-6179,DAASS-6180)	
Purpose: 
1)In my_sql_command_11  order by clause not working as expected its updating lower value even though we are using order by i.e(in the same transaction for the guest ABC, if dates are 2022-09-01 and 2022-09-02). To fix this issue we are going with row_number function instead of the order by in the subquery
2)COMMENTED GUEST_UNIQUE_ID,PROPERTY_CD,MARKET_CD,TRIP_TYPE  since we are using trip_master_id which is an unique column in trip_master table
Modified By : Nambi Arasappan
Modified Date: 10/12/2022
Version: 1.3
Purpose: One property - Multiple Markets Scenario Fix
Modified By : Riya Batra
Modified Date: 11/18/2022
Version: 1.4
Purpose: (DAASS-6474) Duplicate in Trip Detail for one SK tagged to one trip_master_id multiple times
Removed update on TRANSACTION_MODIFIED_DTTM to avoid duplicates
TRANSACTION_MODIFIED_DTTM is mapped from temp table while inserting in trip detail
whereas in my_sql_command_4_1, it was mapped from base table HOTEL_GUEST_RESERVATIONS_FACT, this can create duplicate records where TRANSACTION_MODIFIED_DTTM is different for same SK, if update happened post data is consumed from stream to load temp table

Modified By:Priyanka V
Modified Date:18/08/2023
version:1.5
Purpose:fix for 1sk tagging to 2 guest ids in trip detail-soft deleting the former guest id so that only the current id will be active in trip detail

Modified By : Sunita V
Modified Date: 06/09/2023
Version: 1.5
Purpose: To add property_cd in trip_detail table(search with text DETAIL_PROPERTY_CD for changes)
#####################################################################################
*/

proc_output = "";
proc_step = "";
snowflake.execute( {sqlText: "USE WAREHOUSE " + WAREHOUSE_NAME} );
tag = BATCH_ID + "_HOTEL_DOMAIN_PIPELINE_PROC";
			
snowflake.execute( {sqlText: "ALTER SESSION SET QUERY_TAG = '" + tag + "'" });

snowflake.execute( {sqlText: "CREATE OR REPLACE TABLE DAAS_TEMP.HOTEL_GUEST_RESERVATIONS_FACT_TMP AS SELECT HOTEL_GUEST_RESERVATIONS_FACT_SK, RESERVATION_ID, PROPERTY_CD, CHECK_IN_DTTM, CHECK_OUT_DTTM, DELETE_IND AS DELETE_FLG, UPDATED_DTTM, 0::BIGINT AS GUEST_UNIQUE_ID FROM DAAS_CORE.HOTEL_GUEST_RESERVATIONS_FACT LIMIT 0;"});
snowflake.execute( {sqlText: "CREATE OR REPLACE TABLE DAAS_TEMP.HOTEL_GUEST_RESERVATIONS_FACT_FINAL_TMP AS SELECT *, 'Unmapped' Mappin_Status  FROM DAAS_TEMP.HOTEL_GUEST_RESERVATIONS_FACT_TMP LIMIT 0;"} );
snowflake.execute( {sqlText: "TRUNCATE TABLE DAAS_TEMP.TRIP_HOTEL_RESULT_TMP_0;"});
snowflake.execute( {sqlText: "TRUNCATE TABLE DAAS_TEMP.TRIP_HOTEL_RESULT_TMP;"});
snowflake.execute( {sqlText: "TRUNCATE TABLE DAAS_TEMP.TRIP_HOTEL_RESULT_TEMP1;"});

try
{
	snowflake.execute( {sqlText: "BEGIN;" } );
	proc_step = "Data_Process";
		/*
			1) Identify Valid Winnet_id
				]Note: only transactions with valid Winnet_id will contribute to a trip
			2) Removing invalied property codes 
		*/
		
	my_sql_command_1 =`INSERT INTO DAAS_TEMP.HOTEL_GUEST_RESERVATIONS_FACT_TMP 
	SELECT
		HTL.HOTEL_GUEST_RESERVATIONS_FACT_SK,
		HTL.RESERVATION_ID,
		HTL.PROPERTY_CD,
		HTL.ARRIVAL_DT AS CHECK_IN_DTTM,
		HTL.DEPARTURE_DT AS CHECK_OUT_DTTM,
		CASE WHEN (HTL.DELETE_IND = 'Y' OR HTL.CHECK_IN_DTTM = '1899-12-31') THEN 'Y' ELSE 'N' END AS DELETE_FLG,
		HTL.UPDATED_DTTM,
		HTLV.GUEST_UNIQUE_ID
	FROM
		DAAS_CORE.HOTEL_GUEST_RESERVATIONS_FACT_TRIP_STREAM	HTL
	
/* Handling reservations that are not part of folio table */
/*		(
			SELECT
				HTL.HOTEL_GUEST_RESERVATIONS_FACT_SK,
				HTL.RESERVATION_ID,
				HTL.PROPERTY_CD,
				HTL.ARRIVAL_DT,
				HTL.DEPARTURE_DT,
				HTL.DELETE_IND,
				HTL.CHECK_IN_DTTM,
				HTL.CHECK_OUT_DTTM,
				HTL.RESERVATION_STATUS_CD,
				HTL.UPDATED_DTTM
			FROM
				DAAS_CORE.HOTEL_GUEST_RESERVATIONS_FACT_TRIP_STREAM	HTL
			JOIN
				DAAS_CORE.HOTEL_FOLIO_DETAIL_FACT HFDF
			ON
				HTL.RESERVATION_ID = HFDF.RESERVATION_ID
				AND HTL.PROPERTY_CD = HFDF.PROPERTY_CD
			WHERE
				HTL.METADATA$ACTION = 'INSERT'
				AND HFDF.DELETE_IND = 'N'
			UNION
			SELECT
				HTL.HOTEL_GUEST_RESERVATIONS_FACT_SK,
				HTL.RESERVATION_ID,
				HTL.PROPERTY_CD,
				HTL.ARRIVAL_DT,
				HTL.DEPARTURE_DT,
				HTL.DELETE_IND,
				HTL.CHECK_IN_DTTM,
				HTL.CHECK_OUT_DTTM,
				HTL.RESERVATION_STATUS_CD,
				HTL.UPDATED_DTTM
			FROM
				DAAS_CORE.HOTEL_GUEST_RESERVATIONS_FACT	HTL
			JOIN
				DAAS_CORE.HOTEL_FOLIO_DETAIL_FACT_TRIP_STREAM HFDF
			ON
				HTL.RESERVATION_ID = HFDF.RESERVATION_ID
				AND HTL.PROPERTY_CD = HFDF.PROPERTY_CD
			WHERE
				HFDF.METADATA$ACTION = 'INSERT'
				AND HFDF.DELETE_IND = 'N'
		)HTL   */
	JOIN 
		DAAS_CORE_HOTEL_VW.HOTEL_GUEST_RESERVATIONS_FACT_VW	HTLV 
	ON 
		HTL.HOTEL_GUEST_RESERVATIONS_FACT_SK = HTLV.HOTEL_GUEST_RESERVATIONS_FACT_SK
	JOIN 
		TABLE(DAAS_COMMON.TRIP_CONFIG_UDF('RESERVATION_STATUS_CD','TRIP_TRIGGER','','','HOTEL_OPEN_TRANSACTION',HTL.ARRIVAL_DT::DATE)) VD  
	WHERE
		LENGTH(TRIM(HTL.CHECK_IN_DTTM::DATE)) <> 0 
		AND HTL.CHECK_IN_DTTM::DATE <= (IFF(HTL.CHECK_OUT_DTTM::DATE='1899-12-31', HTL.DEPARTURE_DT, IFF(HTL.CHECK_OUT_DTTM <= CURRENT_DATE, HTL.CHECK_OUT_DTTM, HTL.DEPARTURE_DT)))
		AND HTL.RESERVATION_STATUS_CD <> ''
		AND CONTAINS(VALUE,HTL.RESERVATION_STATUS_CD)
		AND METADATA$ACTION = 'INSERT'
		AND EXISTS
		( 
			SELECT
				1
			FROM	
				DAAS_CORE.TRIP_CONFIG TC
			WHERE
				TC.PROPERTY_CD = HTL.PROPERTY_CD
				AND ACTIVE_FLG = 'Y'
				AND EXISTS
				(
					SELECT
						1
					FROM
						DAAS_COMMON.PROPERTY_MARKET_LKP LKP
					WHERE
						TC.PROPERTY_CD = LKP.PROPERTY_CD 
				) 
		)
	`;
											 
	statement1 = snowflake.createStatement( {sqlText: my_sql_command_1 } );	
	proc_output = my_sql_command_1;
	statement1.execute(); 

	my_sql_command_1_1 = `
	INSERT INTO DAAS_CORE.TRIP_FILTERED_TXNS(TRANSACTION_TABLE_SK, TRANSACTION_TYPE, BATCH_ID)
	SELECT DISTINCT
		HOTEL_GUEST_RESERVATIONS_FACT_SK,
		'HOTEL' AS TRANSACTION_TYPE,
		` + BATCH_ID + ` AS BATCH_ID
	FROM
		DAAS_TEMP.HOTEL_GUEST_RESERVATIONS_FACT_TMP HGRFT
	WHERE
		GUEST_UNIQUE_ID = 0
		AND NOT EXISTS
		(
			SELECT
				1
			FROM
				DAAS_CORE.TRIP_FILTERED_TXNS TFT
			WHERE 
				HGRFT.HOTEL_GUEST_RESERVATIONS_FACT_SK = TFT.TRANSACTION_TABLE_SK
				AND TFT.TRANSACTION_TYPE = 'HOTEL'
				AND TFT.DELETE_IND = 'N'
		)
	`;

	statement1_1 = snowflake.createStatement( {sqlText: my_sql_command_1_1 } );	
	proc_output = my_sql_command_1_1;
	statement1_1.execute(); 

	my_sql_command_1_2 = `DELETE FROM DAAS_TEMP.HOTEL_GUEST_RESERVATIONS_FACT_TMP WHERE GUEST_UNIQUE_ID = 0`;

	statement1_2 = snowflake.createStatement( {sqlText: my_sql_command_1_2 } );	
	proc_output = my_sql_command_1_2;
	statement1_2.execute(); 
					  
	get_Source_Rec_Count = snowflake.execute( {sqlText: "SELECT	COUNT(1) FROM DAAS_TEMP.HOTEL_GUEST_RESERVATIONS_FACT_TMP" } );
	get_Source_Rec_Count.next();		
	Source_Rec_Count = get_Source_Rec_Count.getColumnValue(1);



	/*	updating DELETE_FLG to Y from void and delete_ind records	*/
					
	my_sql_command_2_1 =`DELETE FROM DAAS_TEMP.HOTEL_GUEST_RESERVATIONS_FACT_TMP TMP 
	WHERE 
		TMP.DELETE_FLG = 'Y' 
		AND NOT EXISTS
		(
			SELECT
				1
			FROM
				DAAS_CORE.TRIP_DETAIL TD
			WHERE
				TMP.HOTEL_GUEST_RESERVATIONS_FACT_SK = TD.TRANSACTION_TABLE_SK 
				AND TD.TRANSACTION_TYPE = 'HOTEL'
				AND TD.DELETE_IND = 'N'
		)
	`;	

	statement2_1 = snowflake.createStatement( {sqlText: my_sql_command_2_1 } );
	proc_output = my_sql_command_2_1;				
	statement2_1.execute();
	   
	/*For void/delete records Identify original record (if applicable)  */				
								  
	my_sql_command_3 = `
	INSERT INTO DAAS_TEMP.HOTEL_GUEST_RESERVATIONS_FACT_FINAL_TMP 
	SELECT 
		*, 
		'Unmapped' AS MAPPIN_STATUS 
	FROM 
		DAAS_TEMP.HOTEL_GUEST_RESERVATIONS_FACT_TMP
	`;

	statement3 = snowflake.createStatement( {sqlText: my_sql_command_3 } );				
	proc_output = my_sql_command_3;			
	statement3.execute();
						
	/*	Update Modified Date in trip detail record for the transaction_id(srg_key)	*/

	my_sql_command_4_0 =` 
	UPDATE
		DAAS_TEMP.HOTEL_GUEST_RESERVATIONS_FACT_FINAL_TMP FTMP
	SET
		FTMP.MAPPIN_STATUS = 'mapped'
	FROM
		DAAS_CORE.TRIP_DETAIL TD
	WHERE
		TD.TRANSACTION_TABLE_SK = FTMP.HOTEL_GUEST_RESERVATIONS_FACT_SK   
		AND FTMP.CHECK_IN_DTTM::DATE = TD.TRANSACTION_START_DTTM::DATE  
		AND TD.TRANSACTION_END_DTTM::DATE = FTMP.CHECK_OUT_DTTM::DATE
		AND TD.TRANSACTION_TABLE = 'HOTEL_GUEST_RESERVATIONS_FACT'
		AND FTMP.DELETE_FLG = 'N'
		AND TD.DELETE_IND = 'N'
	`;

	statement_4_0 = snowflake.createStatement( {sqlText: my_sql_command_4_0 } );
	proc_output = my_sql_command_4_0;
	statement_4_0.execute();
	 
	my_sql_command_4 =`
	UPDATE DAAS_CORE.TRIP_DETAIL TD 
	SET
		TD.TRANSACTION_MODIFIED_DTTM = FTMP.UPDATED_DTTM,
		TD.TRANSACTION_END_DTTM = IFF(TD.TRANSACTION_END_DTTM ::DATE <> FTMP.CHECK_OUT_DTTM::DATE, FTMP.CHECK_OUT_DTTM::DATE, TD.TRANSACTION_END_DTTM::DATE),
		TD.BUSINESS_END_DT = IFF(TD.BUSINESS_END_DT ::DATE <> FTMP.CHECK_OUT_DTTM::DATE, FTMP.CHECK_OUT_DTTM::DATE, TD.BUSINESS_END_DT::DATE),
		TD.BATCH_ID = ` + BATCH_ID + `, 
		TD.UPDATED_DTTM = CURRENT_TIMESTAMP,
		TD.UPDATED_BY = CURRENT_USER,
		TD.LAST_DML_CD = 'U'
	FROM
		DAAS_TEMP.HOTEL_GUEST_RESERVATIONS_FACT_FINAL_TMP FTMP
	WHERE
		TRANSACTION_TABLE_SK = FTMP.HOTEL_GUEST_RESERVATIONS_FACT_SK  
		AND TD.TRANSACTION_TABLE = 'HOTEL_GUEST_RESERVATIONS_FACT'
		AND FTMP.DELETE_FLG = 'N'
		AND TD.DELETE_IND = 'N'
		AND EXISTS 
		(
			SELECT 
				1 
			FROM 
				DAAS_CORE.TRIP_MASTER 
			WHERE 
				TD.TRIP_MASTER_ID = TRIP_MASTER.TRIP_MASTER_ID 
				AND TRIP_MASTER.DELETE_IND = 'N'
		)
	`;
					
	statement4 = snowflake.createStatement( {sqlText: my_sql_command_4 } );	
	proc_output = my_sql_command_4;
	statement4.execute();
					
	my_sql_command_4_1 =`
	UPDATE DAAS_CORE.TRIP_DETAIL TD 
	SET
		TD.BATCH_ID = ` + BATCH_ID + `, 
		TD.UPDATED_DTTM = CURRENT_TIMESTAMP,
		TD.UPDATED_BY = CURRENT_USER,
		TD.LAST_DML_CD = 'U'
	FROM
		(
			SELECT 
				HTL.HOTEL_GUEST_RESERVATIONS_FACT_SK, 
				MAX(HTL.CHECK_IN_DTTM) AS CHECK_IN_DTTM, 
				MAX(HTL.CHECK_OUT_DTTM) AS CHECK_OUT_DTTM, 
				MAX(HTL.UPDATED_DTTM) AS UPDATED_DTTM 
			FROM 
				DAAS_CORE.HOTEL_FOLIO_DETAIL_FACT_TRIP_STREAM HFDFS
			JOIN
				DAAS_CORE.HOTEL_GUEST_RESERVATIONS_FACT HTL
			ON
				HFDFS.RESERVATION_ID  =	HTL.RESERVATION_ID
				AND HFDFS.PROPERTY_CD =	HTL.PROPERTY_CD
			WHERE
				HTL.DELETE_IND = 'N'
				AND HFDFS.DELETE_IND = 'N'
			GROUP BY 
				HTL.HOTEL_GUEST_RESERVATIONS_FACT_SK
		) FTMP
	WHERE
		TRANSACTION_TABLE_SK = FTMP.HOTEL_GUEST_RESERVATIONS_FACT_SK  
		AND TD.TRANSACTION_TABLE ='HOTEL_GUEST_RESERVATIONS_FACT'
		AND TD.DELETE_IND = 'N'
	`;				

	statement4_1 = snowflake.createStatement( {sqlText: my_sql_command_4_1 } );	
	proc_output = my_sql_command_4_1;
	statement4_1.execute();

			
	/*
		 Soft delete trip detail record identified in  before step 
	/** adding additional code as a fix for 1sk tagging to 2 guest ids in trip detail-soft deleting the former guest id so that only the current id will be active in trip detail**/

	
				
	my_sql_command_7 =`
	UPDATE 
		DAAS_CORE.TRIP_DETAIL TD
	SET
		DELETE_IND = 'Y', 
		TRANSACTION_SUB_TYPE = CASE WHEN(FTMP.GUEST_UNIQUE_ID <> TM.GUEST_UNIQUE_ID) THEN 'GUEST_CHANGED' ELSE 'VOIDED' END,
		TD.UPDATED_DTTM = CURRENT_TIMESTAMP,
		TD.UPDATED_BY = CURRENT_USER,
		TD.BATCH_ID = ` + BATCH_ID + `,
		TD.LAST_DML_CD = 'U'
	FROM
		DAAS_TEMP.HOTEL_GUEST_RESERVATIONS_FACT_FINAL_TMP FTMP 
	JOIN 
	    (
		 SELECT 
			TRANSACTION_TABLE_SK,
			TRIP_MASTER_ID 
		 FROM 
			DAAS_CORE.TRIP_DETAIL TD 
		 WHERE 
			TD.TRANSACTION_TYPE	= 'HOTEL'
		 AND 
			TD.DELETE_IND =	'N'
		) LKP 
	ON 
		LKP.TRANSACTION_TABLE_SK	= FTMP.HOTEL_GUEST_RESERVATIONS_FACT_SK
	JOIN 
		DAAS_CORE.TRIP_MASTER TM
	ON 
		TM.TRIP_MASTER_ID = LKP.TRIP_MASTER_ID  
	WHERE
		TD.TRANSACTION_TABLE_SK	= FTMP.HOTEL_GUEST_RESERVATIONS_FACT_SK
		AND LKP.TRIP_MASTER_ID = TD.TRIP_MASTER_ID
		AND TD.TRANSACTION_TYPE	= 'HOTEL'
		AND TD.DELETE_IND =	'N'
		AND (FTMP.DELETE_FLG = 'Y' OR FTMP.GUEST_UNIQUE_ID <> TM.GUEST_UNIQUE_ID)
		AND TM.DELETE_IND = 'N'
	`;	
				
	statement7 = snowflake.createStatement( {sqlText: my_sql_command_7 } );
	proc_output = my_sql_command_7;
	statement7.execute();
	
	       					
	/*
		Identify if any other active trip detail records are present for the same  transaction_start_dt and inserting to trip_recalc_queue
	*/				
	my_sql_command_8 =`INSERT INTO DAAS_CORE.TRIP_RECALC_QUEUE 
	( 
		TRIP_MASTER_ID,
		STATUS,
		BATCH_ID,
		CREATED_DTTM,
		CREATED_BY,
		UPDATED_DTTM,
		UPDATED_BY 
	)
	SELECT DISTINCT 
		TRIP_MASTER_ID,
		'PENDING',
		` + BATCH_ID + ` AS BATCH_ID ,
		CURRENT_TIMESTAMP CREATED_DTTM,
		CURRENT_USER CREATED_BY,
		CURRENT_TIMESTAMP UPDATED_DTTM,
		CURRENT_USER UPDATED_BY
	FROM
		DAAS_TEMP.HOTEL_GUEST_RESERVATIONS_FACT_FINAL_TMP FTMP 
	JOIN 
		DAAS_CORE.TRIP_DETAIL TD  
	ON 
		FTMP.HOTEL_GUEST_RESERVATIONS_FACT_SK=TD.TRANSACTION_TABLE_SK 
		AND TD.DELETE_IND = 'Y' 
		AND  TD.TRANSACTION_TYPE = 'HOTEL'
	WHERE 
		(FTMP.DELETE_FLG = 'Y'
		AND NOT EXISTS 
		(
			SELECT
				1
			FROM
				DAAS_CORE.TRIP_DETAIL TD
			WHERE
				TD.TRANSACTION_TABLE_SK = FTMP.HOTEL_GUEST_RESERVATIONS_FACT_SK  
				AND  TD.TRANSACTION_TYPE = 'HOTEL'
				AND FTMP.CHECK_IN_DTTM::DATE = TD.TRANSACTION_START_DTTM::DATE   
				AND TD.DELETE_IND = 'N'
		))OR  TD.TRANSACTION_SUB_TYPE='GUEST_CHANGED'	/**added as part of 1sk tagging to 2 guest ids -soft deleting the former guest id so that only the current id will be active in trip master**/
	`;	
						  
	statement8 = snowflake.createStatement( {sqlText: my_sql_command_8 } );				
	proc_output = my_sql_command_8;
	statement8.execute();
						
						
	/*
		Updating  processed record to mapped status in temp table 
	*/			
						
	my_sql_command_8_1 =`
	UPDATE DAAS_TEMP.HOTEL_GUEST_RESERVATIONS_FACT_FINAL_TMP RFT 
	SET RFT.MAPPIN_STATUS = 'mapped' 
	WHERE DELETE_FLG = 'Y'`;	
		
	statement8_1 = snowflake.createStatement( {sqlText: my_sql_command_8_1 } );
	proc_output = my_sql_command_8_1;
	statement8_1.execute();
		
  /*
	  populate 2  more records ( market and enterprise ) for all unmapped status records 
  */	
						
	my_sql_command_9 =`
	INSERT INTO DAAS_TEMP.TRIP_HOTEL_RESULT_TMP_0
	SELECT DISTINCT
		GUEST_UNIQUE_ID ,
		IFF(COLUMN1 = 'PROPERTY',PROPERTY_CD,'N/A') AS PROPERTY_CD,
		PROPERTY_CD DETAIL_PROPERTY_CD,  
		COLUMN1 AS TRIP_TYPE,
		IFF(COLUMN1 = 'MARKET',MARKET_CD,'N/A') AS MARKET_CD,
		HOTEL_GUEST_RESERVATIONS_FACT_SK AS TRANSACTION_TABLE_SK,
		'HOTEL_GUEST_RESERVATIONS_FACT' AS TRANSACTION_TABLE,
		'HOTEL' AS TRANSACTION_TYPE,
		CHECK_IN_DTTM AS TRANSACTION_START_DTTM,
		CHECK_OUT_DTTM AS TRANSACTION_END_DTTM,
		CHECK_IN_DTTM AS BUSINESS_START_DT,
		CHECK_OUT_DTTM AS BUSINESS_END_DT,
		UPDATED_DTTM AS TRANSACTION_MODIFIED_DTTM
	FROM
	(
		SELECT
			FIN.*,
			COLUMN1,
			'N/A' AS MARKET_CD
		FROM
			DAAS_TEMP.HOTEL_GUEST_RESERVATIONS_FACT_FINAL_TMP FIN
		JOIN 
			(SELECT * FROM (VALUES('PROPERTY'),('ENTERPRISE'))) PME
		/*WHERE
			 FIN.MAPPIN_STATUS = 'Unmapped' */
		WHERE FIN.DELETE_FLG = 'N'
		UNION ALL
		SELECT
			FIN.*,
			'MARKET' AS COLUMN1,
			MARKET_CD
		FROM
			DAAS_TEMP.HOTEL_GUEST_RESERVATIONS_FACT_FINAL_TMP FIN
		LEFT JOIN 
			DAAS_COMMON.PROPERTY_MARKET_LKP 
		ON
			FIN.PROPERTY_CD = PROPERTY_MARKET_LKP.PROPERTY_CD
		/*WHERE
			 FIN.MAPPIN_STATUS = 'Unmapped'  */
		WHERE FIN.DELETE_FLG = 'N'
	)
	`;	

	statement9 = snowflake.createStatement( {sqlText: my_sql_command_9 } );
	proc_output = my_sql_command_9;
	statement9.execute();	
			 
							  
	/*passing above step records to   to ITERATION 3 */			
			 

	my_sql_command_10 =`CALL DAAS_COMMON.TRIP_IDENTIFIER_PROC(` + BATCH_ID + `,'` + SHARD_NAME + `','DAAS_TEMP.TRIP_HOTEL_RESULT_TMP_0','DAAS_TEMP.TRIP_HOTEL_RESULT_TMP','HOTEL');`; 

	statement10 = snowflake.createStatement( {sqlText: my_sql_command_10 } );		
	proc_output = my_sql_command_10;
	statment_trip_result=statement10.execute();	
						 
					
	statment_trip_result.next(); 	
	out=statment_trip_result.getColumnValue(1);

	if(out == "FAILURE") throw "FAILURE_FROM_TRIP_FUNCTION";			
						
						
	/*based on result from iteration 3  we are updating or inserting to trip tables */			

	my_sql_command_11 =`UPDATE
		DAAS_CORE.TRIP_MASTER TM
	SET
		TM.TRIP_END_DT = SPLIT_PART(RT.TRIP_INDICATOR,'|',3),
		TM.UPDATED_DTTM = CURRENT_TIMESTAMP,
		TM.UPDATED_BY = CURRENT_USER,
		TM.BATCH_ID = ` + BATCH_ID + `, 
		TM.LAST_DML_CD = 'U'
	FROM
	(
		SELECT 
			* 
		FROM 
		(
			SELECT 
				RT.GUEST_UNIQUE_ID,
				RT.PROPERTY_CD,
				RT.MARKET_CD,
				RT.TRIP_TYPE,
				RT.TRIP_INDICATOR,
				ROW_NUMBER() OVER (PARTITION BY SPLIT_PART(RT.TRIP_INDICATOR, '|', 2) ORDER BY SPLIT_PART(RT.TRIP_INDICATOR, '|', 3) DESC) RNUMBER 
			FROM 
				DAAS_TEMP.TRIP_HOTEL_RESULT_TMP RT
		)RT 
		WHERE 
			RNUMBER = 1
	)RT
	WHERE
		SPLIT_PART(RT.TRIP_INDICATOR, '|', 1) = 'Y'
		AND TM.TRIP_MASTER_ID = IFF(LENGTH(SPLIT_PART(RT.TRIP_INDICATOR, '|', 2)) = 0, 0, SPLIT_PART(RT.TRIP_INDICATOR, '|', 2))
		AND TM.DELETE_IND = 'N'
		AND LENGTH(SPLIT_PART(RT.TRIP_INDICATOR, '|', 3)) > 0
        AND DATE(SPLIT_PART(RT.TRIP_INDICATOR, '|', 3)) <> TM.TRIP_END_DT		
	`; 
					  
	statement11 = snowflake.createStatement( {sqlText: my_sql_command_11 } );
	proc_output = my_sql_command_11;
	statement11.execute();	
					  
	my_sql_command_12 =`INSERT INTO DAAS_CORE.TRIP_DETAIL 
	( 
		TRIP_DETAIL_ID,
		TRIP_MASTER_ID,
		TRANSACTION_TABLE_SK,
		PROPERTY_CD,
		TRANSACTION_TABLE,
		TRANSACTION_TYPE,
		TRANSACTION_SUB_TYPE,
		TRANSACTION_START_DTTM,
		TRANSACTION_END_DTTM,
		BUSINESS_START_DT,
		BUSINESS_END_DT,
		TRANSACTION_MODIFIED_DTTM,
		DELETE_IND,
		CREATED_DTTM,
		CREATED_BY,
		UPDATED_DTTM,
		UPDATED_BY,
		BATCH_ID,
		LAST_DML_CD	
	)
	SELECT
		DAAS_CORE.TRIP_DETAIL_SEQ.NEXTVAL AS TRIP_DETAIL_ID,
		TRIP_MASTER_ID TRIP_MASTER_ID,
		TRANSACTION_TABLE_SK,
		DETAIL_PROPERTY_CD,
		TRANSACTION_TABLE,
		'HOTEL' TRANSACTION_TYPE,
		'OPENED' TRANSACTION_SUB_TYPE,
		TRANSACTION_START_DTTM,
		TRANSACTION_END_DTTM,
		BUSINESS_START_DT,
		BUSINESS_END_DT,
		TRANSACTION_MODIFIED_DTTM,
		'N' DELETE_IND,
		CURRENT_TIMESTAMP AS CREATED_DTTM,
		CURRENT_USER AS CREATED_BY,
		CURRENT_TIMESTAMP AS UPDATED_DTTM,
		CURRENT_USER AS UPDATED_BY,
		` + BATCH_ID + ` BATCH_ID,
		'I'
	FROM
	(
		SELECT DISTINCT  
			RT.GUEST_UNIQUE_ID,
			RT.PROPERTY_CD,
			RT.DETAIL_PROPERTY_CD,
			RT.TRIP_TYPE,
			RT.MARKET_CD,
			RT.TRANSACTION_TABLE_SK,
			RT.TRANSACTION_TABLE,
			RT.TRANSACTION_TYPE,
			RT.TRANSACTION_START_DTTM,
			RT.TRANSACTION_END_DTTM,
			RT.BUSINESS_START_DT,
			RT.BUSINESS_END_DT,
			RT.TRANSACTION_MODIFIED_DTTM,
			TM.TRIP_MASTER_ID
		FROM
			DAAS_TEMP.TRIP_HOTEL_RESULT_TMP RT
		JOIN 
			DAAS_CORE.TRIP_MASTER TM 
		ON
			RT.GUEST_UNIQUE_ID = TM.GUEST_UNIQUE_ID
			AND TM.TRIP_TYPE = RT.TRIP_TYPE
			AND TM.PROPERTY_CD = RT.PROPERTY_CD
			AND TM.MARKET_CD = RT.MARKET_CD
			AND RT.BUSINESS_START_DT BETWEEN TM.TRIP_START_DT AND TM.TRIP_END_DT
			AND TM.DELETE_IND = 'N'
		 WHERE 
			SPLIT_PART(TRIP_INDICATOR, '|', 1) = 'Y' 
			AND NOT EXISTS 
			(
				SELECT 
					1 
				FROM 
					DAAS_CORE.TRIP_DETAIL TD 
				WHERE 
					RT.TRANSACTION_TABLE_SK = TD.TRANSACTION_TABLE_SK 
					AND TM.TRIP_MASTER_ID = TD.TRIP_MASTER_ID  
					AND TD.TRANSACTION_TABLE = 'HOTEL_GUEST_RESERVATIONS_FACT'
					AND TD.DELETE_IND ='N'
			)
	)
	`;		 

	 
			 
	statement12 = snowflake.createStatement( {sqlText: my_sql_command_12} );
	proc_output = my_sql_command_12;
	statement12.execute();	
			 
	/*
		Updating  processed record to mapped status in temp table 
	*/			
		
	my_sql_command_13 =` UPDATE DAAS_TEMP.HOTEL_GUEST_RESERVATIONS_FACT_FINAL_TMP RFT
	SET
		RFT.MAPPIN_STATUS = 'mapped'
	WHERE
		EXISTS 
	(
		SELECT
			1
		FROM
			DAAS_TEMP.TRIP_HOTEL_RESULT_TMP RT
		WHERE
			RT.TRANSACTION_TABLE_SK = RFT.HOTEL_GUEST_RESERVATIONS_FACT_SK
			AND SPLIT_PART(TRIP_INDICATOR, '|', 1) = 'Y'
	)	
	`;				
				
	statement13 = snowflake.createStatement( {sqlText: my_sql_command_13} );			
	proc_output = my_sql_command_13;
	statement13.execute();		
						
	 /*passing unmapped record to ITERATION 4 SP */
	  
	my_sql_command_14 =`CALL DAAS_COMMON.TRIP_FIND_START_DATE_END_DATE_PROC(` + BATCH_ID + `,'` + SHARD_NAME + `','DAAS_TEMP.TRIP_HOTEL_RESULT_TMP','DAAS_TEMP.TRIP_HOTEL_RESULT_TEMP1')`;	
			 
	statement14 = snowflake.createStatement( {sqlText: my_sql_command_14} );
	proc_output = my_sql_command_14;
	statment_trip_result1=statement14.execute();
					
	statment_trip_result1.next(); 
	out1=statment_trip_result1.getColumnValue(1);

	if(out1 == "FAILURE") throw "FAILURE_FROM_TRIP_FUNCTION";				
		
	 /* based on  ITERATION 4 SP out put we are inserting new trip to trip tables */	

	my_sql_command_15 =`INSERT INTO DAAS_CORE.TRIP_MASTER 
	( 
		TRIP_MASTER_ID ,
		GUEST_UNIQUE_ID ,
		PROPERTY_CD ,
		MARKET_CD ,
		TRIP_TYPE ,
		TRIP_START_DT ,
		TRIP_END_DT ,
		DELETE_IND ,
		CREATED_DTTM ,
		CREATED_BY ,
		UPDATED_DTTM ,
		UPDATED_BY ,
		BATCH_ID,
		LAST_DML_CD
	)
	SELECT
		DAAS_CORE.TRIP_MASTER_SEQ.NEXTVAL AS TRIP_MASTER_ID,
		GUEST_UNIQUE_ID,
		PROPERTY_CD,
		MARKET_CD,
		TRIP_TYPE,
		TRIP_START_DT,
		TRIP_END_DT,
		'N' AS DELETE_IND,
		CURRENT_TIMESTAMP AS CREATED_DTTM,
		CURRENT_USER AS CREATED_BY,
		CURRENT_TIMESTAMP AS UPDATED_DTTM,
		CURRENT_USER AS UPDATED_BY,
		` + BATCH_ID + ` AS BATCH_ID,
		'I'
	FROM
	(
		SELECT DISTINCT 
			GUEST_UNIQUE_ID,
			PROPERTY_CD,
			MARKET_CD,
			TRIP_TYPE,
			TRIP_START_DT,
			TRIP_END_DT
		FROM
			DAAS_TEMP.TRIP_HOTEL_RESULT_TEMP1
	)
	`; 

	statement15 = snowflake.createStatement( {sqlText: my_sql_command_15} );
	proc_output = my_sql_command_15;
	statement15.execute();	
						
	my_sql_command_16 =`INSERT INTO DAAS_CORE.TRIP_DETAIL 
	( 
		TRIP_DETAIL_ID,
		TRIP_MASTER_ID ,
		TRANSACTION_TABLE_SK ,
		PROPERTY_CD,
		TRANSACTION_TABLE ,
		TRANSACTION_TYPE ,
		TRANSACTION_SUB_TYPE ,
		TRANSACTION_START_DTTM ,
		TRANSACTION_END_DTTM ,
		BUSINESS_START_DT ,
		BUSINESS_END_DT ,
		TRANSACTION_MODIFIED_DTTM,
		DELETE_IND ,
		CREATED_DTTM ,
		CREATED_BY ,
		UPDATED_DTTM ,
		UPDATED_BY ,
		BATCH_ID,
		LAST_DML_CD
	)
	SELECT
		DAAS_CORE.TRIP_DETAIL_SEQ.NEXTVAL AS TRIP_DETAIL_ID,
		TRIP_MASTER_ID,
		TRANSACTION_TABLE_SK,
		DETAIL_PROPERTY_CD,
		TRANSACTION_TABLE,
		TRANSACTION_TYPE,
		'OPENED' AS TRANSACTION_SUB_TYPE,
		TRANSACTION_START_DTTM,
		TRANSACTION_END_DTTM,
		BUSINESS_START_DT,
		BUSINESS_END_DT,
		TRANSACTION_MODIFIED_DTTM,
		'N' AS DELETE_IND,
		CURRENT_TIMESTAMP AS CREATED_DTTM,
		CURRENT_USER AS CREATED_BY,
		CURRENT_TIMESTAMP AS UPDATED_DTTM,
		CURRENT_USER AS UPDATED_BY,
		 ` + BATCH_ID + ` AS BATCH_ID,
		'I'
	FROM
	(
		SELECT DISTINCT  		
			RT.GUEST_UNIQUE_ID,
			RT.PROPERTY_CD,
			RT.DETAIL_PROPERTY_CD,
			RT.TRIP_TYPE,
			RT.MARKET_CD,
			RT.TRANSACTION_TABLE_SK,
			RT.TRANSACTION_TABLE,
			RT.TRANSACTION_TYPE,
			RT.TRANSACTION_START_DTTM,
			RT.TRANSACTION_END_DTTM,
			RT.BUSINESS_START_DT,
			RT.BUSINESS_END_DT,
			RT.TRANSACTION_MODIFIED_DTTM,
			RT.MAPPING_STATUS,
			RT.TRIP_START_DT,
			RT.TRIP_END_DT,
			TM.TRIP_MASTER_ID
		FROM
			DAAS_TEMP.TRIP_HOTEL_RESULT_TEMP1 RT
		JOIN 
			DAAS_CORE.TRIP_MASTER TM 
		ON
			RT.GUEST_UNIQUE_ID = TM.GUEST_UNIQUE_ID
			AND TM.TRIP_TYPE = RT.TRIP_TYPE
			AND TM.PROPERTY_CD = RT.PROPERTY_CD
			AND TM.MARKET_CD = RT.MARKET_CD
			AND RT.BUSINESS_START_DT BETWEEN TM.TRIP_start_DT AND TM.TRIP_END_DT
			AND TM.DELETE_IND = 'N'
	)`;		
			 
	statement16 = snowflake.createStatement( {sqlText: my_sql_command_16} );
	proc_output = my_sql_command_16;
	statement16.execute();	
	snowflake.execute( {sqlText: "COMMIT;" } ); 

	/*Identify Core Record_Count based on batch_id, LAST_DML_CD*/

	get_Master_Count = snowflake.execute( {sqlText: "SELECT NVL(SUM(CASE WHEN LAST_DML_CD = 'U' THEN 1 ELSE 0 END), 0) AS UPDATE_COUNT,  NVL(SUM(CASE WHEN LAST_DML_CD = 'I' THEN 1 ELSE 0 END), 0) AS INSERT_COUNT FROM DAAS_CORE.TRIP_MASTER WHERE BATCH_ID = " + BATCH_ID + "" } );

	get_Master_Count.next();
	Master_Update_Count = get_Master_Count.getColumnValue(1);
	Master_Insert_Count = get_Master_Count.getColumnValue(2);

	get_Detail_Count = snowflake.execute( {sqlText: "SELECT NVL(SUM(CASE WHEN LAST_DML_CD = 'U' THEN 1 ELSE 0 END), 0) AS UPDATE_COUNT,  NVL(SUM(CASE WHEN LAST_DML_CD = 'I' THEN 1 ELSE 0 END), 0) AS INSERT_COUNT FROM DAAS_CORE.TRIP_DETAIL WHERE BATCH_ID = " + BATCH_ID + "" } );

	get_Detail_Count.next();
	Detail_Update_Count = get_Detail_Count.getColumnValue(1);
	Detail_Insert_Count = get_Detail_Count.getColumnValue(2);
			
	/*Call Update_Batch_Metrics for each inserting each count metrics*/
			
	call_source_rec_count = snowflake.execute({sqlText: "CALL DAAS_COMMON.BATCH_CONTROL_UPDATE_BATCH_METRICS_PROC('" + BATCH_ID + "' , '" + SHARD_NAME + "', 'Source_Rec_Count', '" + Source_Rec_Count + "');" });
	call_master_insert_count = snowflake.execute({sqlText:"CALL DAAS_COMMON.BATCH_CONTROL_UPDATE_BATCH_METRICS_PROC('" + BATCH_ID + "' , '" + SHARD_NAME + "', 'Master_Insert_Count' , '" + Master_Insert_Count + "');" });
	call_master_update_count = snowflake.execute({sqlText:"CALL DAAS_COMMON.BATCH_CONTROL_UPDATE_BATCH_METRICS_PROC('" + BATCH_ID + "' , '" + SHARD_NAME + "', 'Master_Update_Count' , '" + Master_Update_Count + "');" });
	call_detail_insert_count = snowflake.execute({sqlText:"CALL DAAS_COMMON.BATCH_CONTROL_UPDATE_BATCH_METRICS_PROC('" + BATCH_ID + "' , '" + SHARD_NAME + "', 'Detail_Insert_Count' , '" + Detail_Insert_Count + "');" });
	call_detail_update_count = snowflake.execute({sqlText:"CALL DAAS_COMMON.BATCH_CONTROL_UPDATE_BATCH_METRICS_PROC('" + BATCH_ID + "' , '" + SHARD_NAME + "', 'Detail_Update_Count' , '" + Detail_Update_Count + "');" });

	call_source_rec_count.next();
	call_master_insert_count.next();
	call_master_update_count.next();
	call_detail_insert_count.next();
	call_detail_update_count.next();
			
	get_val_source_count = call_source_rec_count.getColumnValue(1);
	get_val_master_insert_count = call_master_insert_count.getColumnValue(1);
	get_val_master_update_count = call_master_update_count.getColumnValue(1);
	get_val_detail_insert_count = call_detail_insert_count.getColumnValue(1);
	get_val_detail_update_count = call_detail_update_count.getColumnValue(1);

	/*Error Handling if Metrics Update Failed*/
	if (get_val_source_count.includes("SUCCESS") != true || get_val_master_insert_count.includes("SUCCESS") != true || get_val_master_update_count.includes("SUCCESS") != true || get_val_detail_insert_count.includes("SUCCESS") != true || get_val_detail_update_count.includes("SUCCESS") != true) 
	{ 
		proc_output = "SOURCE COUNT METRIC STATUS: " + get_val_source_count + "\nMASTER INSERT COUNT METRIC STATUS: " + get_val_master_insert_count + + "\nMASTER UPDATE COUNT METRIC STATUS: " + get_val_master_update_count +"\nDETAIL INSERT COUNT METRIC STATUS: " + get_val_detail_insert_count + "\nDETAIL UPDATE COUNT METRIC STATUS: " + get_val_detail_update_count +"\nFAILURE RETURNED FROM METRICS";
	}
	else 
	{ 
		proc_output = "SUCCESS";
	}

	/*Commit the Update_Metrics Step */
	snowflake.execute( {sqlText: "COMMIT;" } );

	proc_output = "SUCCESS";
}
catch (err) 
{  
	if( err.message == "FAILURE_FROM_TRIP_FUNCTION") 
	{
		proc_output = "FAILURE_FROM_TRIP_FUNCTION";
	} 
	else 
	{
		proc_output = "FAILURE";
	}
					  
	error_code = "Failed: Code: " + err.code + "  State: " + err.state;
	error_message = "\n  Message: " + err.message + "\nStack Trace:\n" + err.stackTraceTxt;
	error_message = error_message.replace(/["'"]/g, "");
	if ( proc_step == "Data_Process")
	{
		/*CALL BATCH_CONTROL_UPDATE_BATCH_ERROR_LOG_PROC*/
		snowflake.execute( {sqlText: "ROLLBACK;" } );
		my_sql_command_17 = "CALL DAAS_COMMON.BATCH_CONTROL_UPDATE_BATCH_ERROR_LOG_PROC ('" + BATCH_ID + "','" + SHARD_NAME + "','" + error_code + "','" + error_message + "','','','FATAL','" + tag + "_" + proc_step +"')"	
	}
	else 
	{
		snowflake.execute( {sqlText: "ROLLBACK;" } );
		my_sql_command_17 = "CALL DAAS_COMMON.BATCH_CONTROL_UPDATE_BATCH_ERROR_LOG_PROC ('" + BATCH_ID + "','" + SHARD_NAME + "','" + error_code + "','" + error_message + "','','','INFORMATIONAL','" + tag + "_" + proc_step +"')"
	} 
					
	snowflake.execute( {sqlText: my_sql_command_17});
}
return proc_output;
$$ ;