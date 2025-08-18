insert into #PS_DB_Name.$EDW_OS_PROCDB#.email_engage_cur_mn_proc (i_xref_dmid, c_email_engagement_cd, i_ins_run_id)  
select BALOP_22.i_dmid as i_xref_dmid, BALOP_22.c_email_engagement_cd as c_email_engagement_cd, #PS_STREAM_INFO.EDW_JOB_RUN_ID# as i_ins_run_id 
from  
(select BALOP_2.c_email_engagement_cd as c_email_engagement_cd, BALOP_21.i_dmid as i_dmid 
from  
(select BALOP_2.i_rule_id as i_rule_id, BALOP_2.c_email_engagement_cd as c_email_engagement_cd 
from  
(select CAST(i_rule_id AS SMALLINT) as i_rule_id, CAST(c_email_engagement_cd AS CHAR(1)) as c_email_engagement_cd 
from #PS_DB_Name.$EDW_OS_EDWDB#.email_engage_code_by_rule BALOP_1) BALOP_2) BALOP_2 
inner join  
(select BALOP_18.i_dmid as i_dmid, BALOP_20.i_rule_id as i_rule_id 
from  
(select BALOP_15.i_dmid as i_dmid, BALOP_17.i_measure_id as i_measure_id 
from  
(select i_dmid as i_dmid, 'MN_PROC_DEFAULT' as c_job_cd 
from  
(select BALOP_13.i_dmid as i_dmid 
from  
(select BALOP_10.i_dmid as i_dmid,  CASE WHEN (BALOP_12.i_xref_dmid_2 is null ) THEN (0) ELSE (BALOP_12.i_xref_dmid_2) END as i_xref_dmid 
from  
(select BALOP_9.i_dmid as i_dmid, BALOP_9.i_xref_dmid as i_xref_dmid 
from  
	(select RemdupAliasC25.i_dmid as i_dmid, RemdupAliasC25.i_xref_dmid as i_xref_dmid, RemdupAliasC25.RemdupPivotColumn26 as RemdupPivotColumn26 
	from 
		(select RemdupAliasA23.i_dmid as i_dmid, MIN(RemdupAliasA23.RemdupPivotColumn26) as RemdupPivotColumn26 
from  
(select BALOP_9.i_dmid as i_dmid, BALOP_9.i_xref_dmid as i_xref_dmid, sum(1) over(order by i_dmid asc rows unbounded preceding) as RemdupPivotColumn26 
from  
(select  CASE WHEN (i_dmid_x > 1) THEN (i_dmid_x) ELSE (i_dmid_e) END as i_dmid,  CASE WHEN (i_dmid_x > 1) THEN (i_dmid_x) ELSE (i_dmid_e) END as i_xref_dmid 
from  
(select BALOP_5.i_dmid_e as i_dmid_e,  CASE WHEN (BALOP_7.i_dmid_x is null ) THEN (0) ELSE (BALOP_7.i_dmid_x) END as i_dmid_x 
from  
(select CAST(i_dmid AS DECIMAL(11,0)) as i_dmid_e, CAST(i_dmid AS DECIMAL(11,0)) as i_xref_dmid 
from #PS_DB_Name.$EDW_OS_EDWDB#.gst_email BALOP_4) BALOP_5 
left outer join  
(select CAST(i_dmid AS DECIMAL(11,0)) as i_dmid_x, CAST(i_xref_dmid AS DECIMAL(11,0)) as i_xref_dmid 
from #PS_DB_Name.$EDW_OS_EDWDB#.gst_cons_xref BALOP_6) BALOP_7 
on (BALOP_5.i_xref_dmid = BALOP_7.i_xref_dmid)) BALOP_8) BALOP_9) RemdupAliasA23 
group by RemdupAliasA23.i_dmid) RemdupAliasB24 
		inner join 
		(select BALOP_9.i_dmid as i_dmid, BALOP_9.i_xref_dmid as i_xref_dmid, sum(1) over(order by i_dmid asc rows unbounded preceding) as RemdupPivotColumn26 
from  
(select  CASE WHEN (i_dmid_x > 1) THEN (i_dmid_x) ELSE (i_dmid_e) END as i_dmid,  CASE WHEN (i_dmid_x > 1) THEN (i_dmid_x) ELSE (i_dmid_e) END as i_xref_dmid 
from  
(select BALOP_5.i_dmid_e as i_dmid_e,  CASE WHEN (BALOP_7.i_dmid_x is null ) THEN (0) ELSE (BALOP_7.i_dmid_x) END as i_dmid_x 
from  
(select CAST(i_dmid AS DECIMAL(11,0)) as i_dmid_e, CAST(i_dmid AS DECIMAL(11,0)) as i_xref_dmid 
from #PS_DB_Name.$EDW_OS_EDWDB#.gst_email BALOP_4) BALOP_5 
left outer join  
(select CAST(i_dmid AS DECIMAL(11,0)) as i_dmid_x, CAST(i_xref_dmid AS DECIMAL(11,0)) as i_xref_dmid 
from #PS_DB_Name.$EDW_OS_EDWDB#.gst_cons_xref BALOP_6) BALOP_7 
on (BALOP_5.i_xref_dmid = BALOP_7.i_xref_dmid)) BALOP_8) BALOP_9) RemdupAliasC25 
		on (RemdupAliasB24.i_dmid = RemdupAliasC25.i_dmid) and (RemdupAliasB24.RemdupPivotColumn26 = RemdupAliasC25.RemdupPivotColumn26)) BALOP_9) BALOP_10 
left outer join  
(select CAST(i_xref_dmid AS DECIMAL(11,0)) as i_xref_dmid, CAST(i_xref_dmid AS DECIMAL(11,0)) as i_xref_dmid_2 
from #PS_DB_Name.$EDW_OS_PROCDB#.email_engage_cur_mn_proc BALOP_11) BALOP_12 
on (BALOP_10.i_xref_dmid = BALOP_12.i_xref_dmid)) BALOP_13 
where BALOP_13.i_dmid <> BALOP_13.i_xref_dmid) BALOP_14) BALOP_15 
inner join  
(select CAST(i_measure_id AS SMALLINT) as i_measure_id, CAST(c_job_cd AS VARCHAR(250)) as c_job_cd 
from #PS_DB_Name.$EDW_OS_EDWDB#.email_engage_measure BALOP_16) BALOP_17 
on (BALOP_15.c_job_cd = BALOP_17.c_job_cd)) BALOP_18 
inner join  
(select CAST(i_rule_id AS SMALLINT) as i_rule_id, CAST(i_measure_id AS SMALLINT) as i_measure_id 
from #PS_DB_Name.$EDW_OS_EDWDB#.email_engage_rule BALOP_19) BALOP_20 
on (BALOP_18.i_measure_id = BALOP_20.i_measure_id)) BALOP_21 
on (BALOP_2.i_rule_id = BALOP_21.i_rule_id)) BALOP_22;
COMMIT WORK