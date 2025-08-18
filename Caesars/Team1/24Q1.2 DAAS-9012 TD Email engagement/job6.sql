insert into #PS_DB_Name.$EDW_OS_EDWDB#.email_engage_hist (i_xref_dmid, c_before_email_engagement_cd, c_current_email_engagement_cd, i_ins_run_id)  
select BALOP_12.i_xref_dmid as i_xref_dmid, BALOP_12.c_before_email_engagement_cd as c_before_email_engagement_cd, BALOP_12.c_current_email_engagement_cd as c_current_email_engagement_cd, #PS_STREAM_INFO.EDW_JOB_RUN_ID# as i_ins_run_id 
from  
(select  CASE WHEN ( CASE WHEN (i_xref_dmid_cur is null ) THEN (0) ELSE (i_xref_dmid_cur) END > 1) THEN (i_xref_dmid_cur) ELSE (i_xref_dmid_prev) END as i_xref_dmid, c_ee_prev_cd as c_before_email_engagement_cd, c_ee_cur_cd as c_current_email_engagement_cd 
from  
(select BALOP_10.i_xref_dmid_cur as i_xref_dmid_cur, BALOP_10.c_ee_cur_cd as c_ee_cur_cd, BALOP_10.i_xref_dmid_prev as i_xref_dmid_prev, BALOP_10.c_ee_prev_cd as c_ee_prev_cd 
from  
(select  CASE WHEN (BALOP_2.i_xref_dmid is null ) THEN (0) ELSE (BALOP_2.i_xref_dmid) END as i_xref_dmid_cur,  CASE WHEN (BALOP_2.c_ee_cur_cd is null ) THEN ('') ELSE (BALOP_2.c_ee_cur_cd) END as c_ee_cur_cd,  CASE WHEN (BALOP_9.i_xref_dmid is null ) THEN (0) ELSE (BALOP_9.i_xref_dmid) END as i_xref_dmid_prev,  CASE WHEN (BALOP_9.c_ee_prev_cd is null ) THEN ('') ELSE (BALOP_9.c_ee_prev_cd) END as c_ee_prev_cd 
from  
(select CAST(i_xref_dmid AS DECIMAL(11,0)) as i_xref_dmid, CAST(c_email_engagement_cd AS CHAR(1)) as c_ee_cur_cd 
from #PS_DB_Name.$EDW_OS_PROCDB#.email_engage_cur_mn_proc BALOP_1) BALOP_2 
full outer join  
(select BALOP_8.i_xref_dmid as i_xref_dmid, BALOP_8.c_ee_prev_cd as c_ee_prev_cd 
from  
	(select RemdupAliasC15.i_xref_dmid as i_xref_dmid, RemdupAliasC15.c_ee_prev_cd as c_ee_prev_cd, RemdupAliasC15.RemdupPivotColumn16 as RemdupPivotColumn16 
	from 
		(select RemdupAliasA13.i_xref_dmid as i_xref_dmid, MIN(RemdupAliasA13.RemdupPivotColumn16) as RemdupPivotColumn16 
from  
(select BALOP_8.i_xref_dmid as i_xref_dmid, BALOP_8.c_ee_prev_cd as c_ee_prev_cd, BALOP_8.i_rank as i_rank, sum(1) over(order by i_xref_dmid asc,i_rank asc rows unbounded preceding) as RemdupPivotColumn16 
from  
(select  CASE WHEN (i_dmid is not null ) THEN (i_dmid) ELSE (i_xref_dmid) END as i_xref_dmid, c_email_engagement_cd as c_ee_prev_cd,  CASE WHEN (c_email_engagement_cd = 'H') THEN (1) WHEN (c_email_engagement_cd = 'M') THEN (2) WHEN (c_email_engagement_cd = 'L') THEN (3) ELSE (0) END as i_rank 
from  
(select BALOP_4.i_xref_dmid as i_xref_dmid, BALOP_4.c_email_engagement_cd as c_email_engagement_cd, BALOP_6.i_dmid as i_dmid 
from  
(select CAST(i_xref_dmid AS DECIMAL(11,0)) as i_xref_dmid, CAST(c_email_engagement_cd AS CHAR(1)) as c_email_engagement_cd 
from #PS_DB_Name.$EDW_OS_PROCDB#.email_engage_prev_mn_proc BALOP_3) BALOP_4 
left outer join  
(select CAST(i_dmid AS DECIMAL(11,0)) as i_dmid, CAST(i_xref_dmid AS DECIMAL(11,0)) as i_xref_dmid 
from #PS_DB_Name.$EDW_OS_EDWDB#.gst_cons_xref BALOP_5) BALOP_6 
on (BALOP_4.i_xref_dmid = BALOP_6.i_xref_dmid)) BALOP_7) BALOP_8) RemdupAliasA13 
group by RemdupAliasA13.i_xref_dmid) RemdupAliasB14 
		inner join 
		(select BALOP_8.i_xref_dmid as i_xref_dmid, BALOP_8.c_ee_prev_cd as c_ee_prev_cd, BALOP_8.i_rank as i_rank, sum(1) over(order by i_xref_dmid asc,i_rank asc rows unbounded preceding) as RemdupPivotColumn16 
from  
(select  CASE WHEN (i_dmid is not null ) THEN (i_dmid) ELSE (i_xref_dmid) END as i_xref_dmid, c_email_engagement_cd as c_ee_prev_cd,  CASE WHEN (c_email_engagement_cd = 'H') THEN (1) WHEN (c_email_engagement_cd = 'M') THEN (2) WHEN (c_email_engagement_cd = 'L') THEN (3) ELSE (0) END as i_rank 
from  
(select BALOP_4.i_xref_dmid as i_xref_dmid, BALOP_4.c_email_engagement_cd as c_email_engagement_cd, BALOP_6.i_dmid as i_dmid 
from  
(select CAST(i_xref_dmid AS DECIMAL(11,0)) as i_xref_dmid, CAST(c_email_engagement_cd AS CHAR(1)) as c_email_engagement_cd 
from #PS_DB_Name.$EDW_OS_PROCDB#.email_engage_prev_mn_proc BALOP_3) BALOP_4 
left outer join  
(select CAST(i_dmid AS DECIMAL(11,0)) as i_dmid, CAST(i_xref_dmid AS DECIMAL(11,0)) as i_xref_dmid 
from #PS_DB_Name.$EDW_OS_EDWDB#.gst_cons_xref BALOP_5) BALOP_6 
on (BALOP_4.i_xref_dmid = BALOP_6.i_xref_dmid)) BALOP_7) BALOP_8) RemdupAliasC15 
		on (RemdupAliasB14.i_xref_dmid = RemdupAliasC15.i_xref_dmid) and (RemdupAliasB14.RemdupPivotColumn16 = RemdupAliasC15.RemdupPivotColumn16)) BALOP_8) BALOP_9 
on (BALOP_2.i_xref_dmid = BALOP_9.i_xref_dmid)) BALOP_10 
where ((BALOP_10.c_ee_cur_cd <> BALOP_10.c_ee_prev_cd) or ((BALOP_10.c_ee_cur_cd is null ) and (BALOP_10.c_ee_prev_cd is not null ))) or ((BALOP_10.c_ee_cur_cd is not null ) and (BALOP_10.c_ee_prev_cd is null ))) BALOP_11) BALOP_12;
