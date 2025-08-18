LOCKING #PS_DB_Name.$EDW_OS_PROCDB#.email_engage_cur_mn_proc FOR ACCESS
select    c_email_engagement_cd, CAST(count(*) AS INTEGER) i_member_cnt from #PS_DB_Name.$EDW_OS_PROCDB#.email_engage_cur_mn_proc
group by c_email_engagement_cd; 


INSERT INTO #PS_DB_Name.$EDW_OS_EDWDB#.email_engage_stats
(    i_ins_run_id  , c_email_engagement_cd,      i_member_cnt  , c_process_cd)
VALUES(    #PS_STREAM_INFO.EDW_JOB_RUN_ID#  , ORCHESTRATE.c_email_engagement_cd  , ORCHESTRATE.i_member_cnt  , 'M');
