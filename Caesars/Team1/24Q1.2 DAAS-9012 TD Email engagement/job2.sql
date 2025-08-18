insert into #PS_DB_Name.$EDW_OS_PROCDB#.email_engage_cur_mn_proc (i_xref_dmid, c_email_engagement_cd, i_ins_run_id)  
select BALOP_11.i_xref_dmid as i_xref_dmid, BALOP_11.c_email_engagement_cd as c_email_engagement_cd, BALOP_11.i_ins_run_id as i_ins_run_id 
from  
(select BALOP_10.i_xref_dmid as i_xref_dmid, BALOP_10.c_email_engagement_cd as c_email_engagement_cd, BALOP_10.i_ins_run_id as i_ins_run_id 
from  
	(select RemdupAliasC14.i_xref_dmid as i_xref_dmid, RemdupAliasC14.c_email_engagement_cd as c_email_engagement_cd, RemdupAliasC14.i_ins_run_id as i_ins_run_id, RemdupAliasC14.RemdupPivotColumn15 as RemdupPivotColumn15 
	from 
		(select RemdupAliasA12.i_xref_dmid as i_xref_dmid, MIN(RemdupAliasA12.RemdupPivotColumn15) as RemdupPivotColumn15 
from  
(select BALOP_10.i_xref_dmid as i_xref_dmid, BALOP_10.c_email_engagement_cd as c_email_engagement_cd, BALOP_10.i_rank_cd as i_rank_cd, BALOP_10.i_ins_run_id as i_ins_run_id, sum(1) over(order by i_xref_dmid asc,i_rank_cd asc rows unbounded preceding) as RemdupPivotColumn15 
from  
(select i_xref_dmid as i_xref_dmid, c_email_engagement_cd as c_email_engagement_cd,  CASE WHEN (c_email_engagement_cd = 'L') THEN (1) WHEN (c_email_engagement_cd = 'M') THEN (2) WHEN (c_email_engagement_cd = 'H') THEN (3) ELSE (0) END as i_rank_cd, '#PS_STREAM_INFO.EDW_JOB_RUN_ID#' as i_ins_run_id 
from  
(select BALOP_6.i_xref_dmid as i_xref_dmid, BALOP_8.c_email_engagement_cd as c_email_engagement_cd 
from  
(select BALOP_5.i_xref_dmid as i_xref_dmid, BALOP_5.i_rule_id as i_rule_id 
from  
(select BALOP_4.i_xref_dmid as i_xref_dmid, BALOP_4.i_rule_id as i_rule_id, BALOP_4.i_measure_id_cnt as i_measure_id_cnt,  CASE WHEN (SUM(BALOP_4.i_passed_nbr) is null ) THEN (0) ELSE (SUM(BALOP_4.i_passed_nbr)) END as i_tt_passed_nbr 
from  
(select BALOP_2.i_xref_dmid as i_xref_dmid,  CASE WHEN (BALOP_3.i_rule_id is null ) THEN (0) ELSE (BALOP_3.i_rule_id) END as i_rule_id, BALOP_2.i_measure_id as i_measure_id, BALOP_2.c_passed_cd as c_passed_cd,  CASE WHEN (BALOP_3.i_measure_id_cnt is null ) THEN (0) ELSE (BALOP_3.i_measure_id_cnt) END as i_measure_id_cnt, BALOP_2.i_passed_nbr as i_passed_nbr 
from  
(select CAST(i_xref_dmid AS DECIMAL(11,0)) as i_xref_dmid, CAST(i_measure_id AS SMALLINT) as i_measure_id, CAST(c_passed_cd AS CHAR(1)) as c_passed_cd, CAST(i_passed_nbr AS SMALLINT) as i_passed_nbr 
from #PS_DB_Name.$EDW_OS_STAGINGDB#.email_engage_measure_aggr_one_stg BALOP_1) BALOP_2 
left outer join  
(select a. i_rule_id , a.i_measure_id , cast ( b.i_measure_id_cnt as integer ) as i_measure_id_cnt from #PS_DB_Name.$EDW_OS_EDWDB#.email_engage_rule a left join ( select i_rule_id , COUNT( * ) as i_measure_id_cnt from #PS_DB_Name.$EDW_OS_EDWDB#.email_engage_rule b group by 1 ) b on a.i_rule_id = b.i_rule_id where c_passed_cd = 'Y') BALOP_3 
on (BALOP_2.i_measure_id = BALOP_3.i_measure_id)) BALOP_4 
group by i_xref_dmid, i_rule_id, i_measure_id_cnt) BALOP_5 
where BALOP_5.i_tt_passed_nbr = BALOP_5.i_measure_id_cnt) BALOP_6 
inner join  
(select CAST(i_rule_id AS SMALLINT) as i_rule_id, CAST(c_email_engagement_cd AS CHAR(1)) as c_email_engagement_cd 
from #PS_DB_Name.$EDW_OS_EDWDB#.email_engage_code_by_rule BALOP_7) BALOP_8 
on (BALOP_6.i_rule_id = BALOP_8.i_rule_id)) BALOP_9) BALOP_10) RemdupAliasA12 
group by RemdupAliasA12.i_xref_dmid) RemdupAliasB13 
		inner join 
		(select BALOP_10.i_xref_dmid as i_xref_dmid, BALOP_10.c_email_engagement_cd as c_email_engagement_cd, BALOP_10.i_rank_cd as i_rank_cd, BALOP_10.i_ins_run_id as i_ins_run_id, sum(1) over(order by i_xref_dmid asc,i_rank_cd asc rows unbounded preceding) as RemdupPivotColumn15 
from  
(select i_xref_dmid as i_xref_dmid, c_email_engagement_cd as c_email_engagement_cd,  CASE WHEN (c_email_engagement_cd = 'L') THEN (1) WHEN (c_email_engagement_cd = 'M') THEN (2) WHEN (c_email_engagement_cd = 'H') THEN (3) ELSE (0) END as i_rank_cd, '#PS_STREAM_INFO.EDW_JOB_RUN_ID#' as i_ins_run_id 
from  
(select BALOP_6.i_xref_dmid as i_xref_dmid, BALOP_8.c_email_engagement_cd as c_email_engagement_cd 
from  
(select BALOP_5.i_xref_dmid as i_xref_dmid, BALOP_5.i_rule_id as i_rule_id 
from  
(select BALOP_4.i_xref_dmid as i_xref_dmid, BALOP_4.i_rule_id as i_rule_id, BALOP_4.i_measure_id_cnt as i_measure_id_cnt,  CASE WHEN (SUM(BALOP_4.i_passed_nbr) is null ) THEN (0) ELSE (SUM(BALOP_4.i_passed_nbr)) END as i_tt_passed_nbr 
from  
(select BALOP_2.i_xref_dmid as i_xref_dmid,  CASE WHEN (BALOP_3.i_rule_id is null ) THEN (0) ELSE (BALOP_3.i_rule_id) END as i_rule_id, BALOP_2.i_measure_id as i_measure_id, BALOP_2.c_passed_cd as c_passed_cd,  CASE WHEN (BALOP_3.i_measure_id_cnt is null ) THEN (0) ELSE (BALOP_3.i_measure_id_cnt) END as i_measure_id_cnt, BALOP_2.i_passed_nbr as i_passed_nbr 
from  
(select CAST(i_xref_dmid AS DECIMAL(11,0)) as i_xref_dmid, CAST(i_measure_id AS SMALLINT) as i_measure_id, CAST(c_passed_cd AS CHAR(1)) as c_passed_cd, CAST(i_passed_nbr AS SMALLINT) as i_passed_nbr 
from #PS_DB_Name.$EDW_OS_STAGINGDB#.email_engage_measure_aggr_one_stg BALOP_1) BALOP_2 
left outer join  
(select a. i_rule_id , a.i_measure_id , cast ( b.i_measure_id_cnt as integer ) as i_measure_id_cnt from #PS_DB_Name.$EDW_OS_EDWDB#.email_engage_rule a left join ( select i_rule_id , COUNT( * ) as i_measure_id_cnt from #PS_DB_Name.$EDW_OS_EDWDB#.email_engage_rule b group by 1 ) b on a.i_rule_id = b.i_rule_id where c_passed_cd = 'Y') BALOP_3 
on (BALOP_2.i_measure_id = BALOP_3.i_measure_id)) BALOP_4 
group by i_xref_dmid, i_rule_id, i_measure_id_cnt) BALOP_5 
where BALOP_5.i_tt_passed_nbr = BALOP_5.i_measure_id_cnt) BALOP_6 
inner join  
(select CAST(i_rule_id AS SMALLINT) as i_rule_id, CAST(c_email_engagement_cd AS CHAR(1)) as c_email_engagement_cd 
from #PS_DB_Name.$EDW_OS_EDWDB#.email_engage_code_by_rule BALOP_7) BALOP_8 
on (BALOP_6.i_rule_id = BALOP_8.i_rule_id)) BALOP_9) BALOP_10) RemdupAliasC14 
		on (RemdupAliasB13.i_xref_dmid = RemdupAliasC14.i_xref_dmid) and (RemdupAliasB13.RemdupPivotColumn15 = RemdupAliasC14.RemdupPivotColumn15)) BALOP_10) BALOP_11;
