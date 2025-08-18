LOCK ${EDW_OS_PROCDB}.email_engage_prev_mn_proc FOR ACCESS
SELECT *
FROM   
(
SELECT MAX(d_timestamp) AS d_prev_mn_dt 
FROM ${EDW_OS_PROCDB}.email_engage_prev_mn_proc
) eep
WHERE EXTRACT(MONTH FROM eep.d_prev_mn_dt) = EXTRACT(MONTH FROM CAST('${EDW_ORDER_DATE}' AS DATE FORMAT 'YYYYMMDD') )
AND   EXTRACT(YEAR FROM eep.d_prev_mn_dt)  = EXTRACT(YEAR FROM CAST('${EDW_ORDER_DATE}' AS DATE FORMAT 'YYYYMMDD') )
;

.IF ERRORCODE <> 0 THEN EXIT ERRORCODE;
.IF ACTIVITYCOUNT = 0 THEN .GOTO OK_PROCEED ;

.LABEL ALREADY_RAN
.REMARK "=== The month of MAX d_timestamp in the previous month table is the same as current_month.";
.REMARK "=== The monthly process has already run for this month. ===";
.QUIT 8;

.LABEL OK_PROCEED
.REMARK "=== Delete the previous month table and insert the previous month data for comparison.";

LOCK ${EDW_OS_PROCDB}.email_engage_cur_mn_proc FOR ACCESS
DELETE
FROM  ${EDW_OS_PROCDB}.email_engage_prev_mn_proc;
 INSERT INTO  ${EDW_OS_PROCDB}.email_engage_prev_mn_proc 
SELECT *
FROM  ${EDW_OS_PROCDB}.email_engage_cur_mn_proc;
