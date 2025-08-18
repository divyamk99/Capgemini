1 -> Markers, COMP:  Delete_ind = 'N' Filter needs to be added -- DONE
2 -> Delete all the records which are having Transaction_sub_type = 'VOIDED' and DELETE_IND = 'N' -- DONE
3 -> All domains: Need to add a filter to get only >=2018 records -- Discuss with Debi
4 -> Delete all the records which are having less than 2017 and business_start_dt or end_dt -- DONE
5 -> Business_start_dt in 2017 and Business_end_dt in 2018 --Need to find solution


-- 2. Delete all the records which are having Transaction_sub_type = 'VOIDED' and DELETE_IND = 'N' -- DONE
UPDATE DAAS_CORE.TRIP_DETAIL 
SET DELETE_IND='Y'
WHERE DELETE_IND='N'
AND TRANSACTION_SUB_TYPE <> 'OPENED';

--4. Delete all the records which are having less than 2017 and business_start_dt or end_dt 
SELECT COUNT(DISTINCT TRIP_MASTER_ID) 
FROM DAAS_CORE.TRIP_DETAIL 
WHERE YEAR(BUSINESS_START_DT)<=2017 
AND YEAR(BUSINESS_END_DT)<=2017 
AND DELETE_IND='N';
--350719(UAT)

UPDATE DAAS_CORE.TRIP_DETAIL 
SET DELETE_IND='Y'
WHERE YEAR(BUSINESS_START_DT)<=2017 
AND YEAR(BUSINESS_END_DT)<=2017 
AND DELETE_IND='N';


---Offer fix try
CALL DAAS_COMMON.QUERIES_EXECUTER('UPDATE DAAS_CORE.TRIP_DETAIL TD
SET TD.REPLAY_COUNTER=COALESCE(TD.REPLAY_COUNTER,0)+1
FROM DAAS_CORE.TRIP_MASTER TM 
WHERE TM.TRIP_MASTER_ID = TD.TRIP_MASTER_ID
AND TD.DELETE_IND=\'N\'
AND TM.DELETE_IND=\'N\'
AND GUEST_UNIQUE_ID IN(19605985756,10101451802,12301006795,12301019103,13600406456,
                       15300058440,13102360828,13606584394,11401734164);');
					   
EXECUTE TASK DAAS_COMMON.TRIP_SUMMARY_WRAPPER_ONDEMAND_TASK;   


05/12 - Hotel extra Sk cleanup
07/12 - Offers extra SK cleanup
08/12, 11/12, 12/12 - Ratings extra SK cleanup

--Pre requisite for Metric mismatch

Delete Records prior to Business_strt_dt and Business_end_dt less than 2017


Delete_ind = 'N' Filter needs to be added --comp & markers domain pipeline
Delete all the records which are having Transaction_sub_type <> 'OPENED' and DELETE_IND = 'N'
All domains: Need to add a filter to get only >=2018 records in domain pipeline


--Collect All Domain GUEST_UNIQUE_ID having discrepency in 1st temp table
--Collect All TRIP_MASTER_ID for above collected GUEST_UNIQUE_ID in 2nd temp table
--Delete records for TRIP tables for TRIP_MASTER_ID in 2nd temp table
--Replay all domain fact table for GUEST_UNIQUE_ID in 1st temp table


CREATE OR REPLACE TABLE DAAS_TEMP.METRIC_TRIP_MASTER_ID
AS SELECT TRIP_MASTER_ID,DENSE_RANK() OVER(ORDER BY TRIP_MASTER_ID) AS RANK
FROM DAAS_CORE.TRIP_MASTER 
WHERE GUEST_UNIQUE_ID IN (SELECT DISTINCT GUEST_UNIQUE_ID FROM DAAS_TEMP.METRIC_GUID);

--RATINGS, HOTEL, COMP(replayed on 26th)