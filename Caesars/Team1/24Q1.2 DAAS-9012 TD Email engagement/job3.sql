insert into #PS_DB_Name.$EDW_OS_STAGINGDB#.email_engage_measure_aggr_one_stg (i_xref_dmid, i_measure_id, c_action_cd, f_measure_qty, i_total_sent, i_total_opens, i_total_clicks, f_action_pct, c_passed_cd, i_passed_nbr)  
select BALOP_17.i_xref_dmid as i_xref_dmid, BALOP_17.i_measure_id as i_measure_id, BALOP_17.c_action_cd as c_action_cd, BALOP_17.f_measure_qty as f_measure_qty, BALOP_17.i_total_sent as i_total_sent, BALOP_17.i_total_opens as i_total_opens, BALOP_17.i_total_clicks as i_total_clicks, BALOP_17.f_action_pct as f_action_pct, BALOP_17.c_passed_cd as c_passed_cd, BALOP_17.i_passed_nbr as i_passed_nbr 
from  
(select i_dmid as i_xref_dmid, i_measure_id as i_measure_id, c_action_cd as c_action_cd, f_measure_qty as f_measure_qty, i_total_sent as i_total_sent, i_total_opens as i_total_opens, i_total_clicks as i_total_clicks,  CASE WHEN (c_action_cd = 'O') THEN ((i_total_opens * 100.00) / i_total_sent) ELSE ((i_total_clicks * 100.00) / i_total_sent) END as f_action_pct,  CASE WHEN ( CASE WHEN (c_action_cd = 'O') THEN ((i_total_opens * 100.00) / i_total_sent) ELSE ((i_total_clicks * 100.00) / i_total_sent) END <= f_measure_qty) THEN ('Y') ELSE ('N') END as c_passed_cd,  CASE WHEN ( CASE WHEN (c_action_cd = 'O') THEN ((i_total_opens * 100.00) / i_total_sent) ELSE ((i_total_clicks * 100.00) / i_total_sent) END <= f_measure_qty) THEN (1) ELSE (0) END as i_passed_nbr 
from  
(select BALOP_15.i_dmid as i_dmid, BALOP_15.i_measure_id as i_measure_id, BALOP_15.c_action_cd as c_action_cd, BALOP_15.f_measure_qty as f_measure_qty,  CASE WHEN (SUM(BALOP_15.i_total_sent) is null ) THEN (0) ELSE (SUM(BALOP_15.i_total_sent)) END as i_total_sent,  CASE WHEN (SUM(BALOP_15.i_total_opens) is null ) THEN (0) ELSE (SUM(BALOP_15.i_total_opens)) END as i_total_opens,  CASE WHEN (SUM(BALOP_15.i_total_clicks) is null ) THEN (0) ELSE (SUM(BALOP_15.i_total_clicks)) END as i_total_clicks 
from  
(select BALOP_14.i_dmid as i_dmid, BALOP_14.i_measure_id as i_measure_id, BALOP_14.c_action_cd as c_action_cd, BALOP_14.f_measure_qty as f_measure_qty, BALOP_14.i_total_sent as i_total_sent, BALOP_14.i_total_opens as i_total_opens, BALOP_14.i_total_clicks as i_total_clicks 
from  
(select BALOP_11.i_dmid as i_dmid,  CASE WHEN (BALOP_13.i_measure_id is null ) THEN (0) ELSE (BALOP_13.i_measure_id) END as i_measure_id,  CASE WHEN (BALOP_13.c_action_cd is null ) THEN ('') ELSE (BALOP_13.c_action_cd) END as c_action_cd, BALOP_11.c_offer_flag as c_offer_flag,  CASE WHEN (BALOP_13.i_months_back_from is null ) THEN (0) ELSE (BALOP_13.i_months_back_from) END as i_months_back_from, BALOP_11.i_months_back as i_months_back,  CASE WHEN (BALOP_13.i_months_back_to is null ) THEN (0) ELSE (BALOP_13.i_months_back_to) END as i_months_back_to,  CASE WHEN (BALOP_13.f_measure_qty is null ) THEN (0) ELSE (BALOP_13.f_measure_qty) END as f_measure_qty, BALOP_11.i_total_sent as i_total_sent, BALOP_11.i_total_opens as i_total_opens, BALOP_11.i_total_clicks as i_total_clicks 
from  
(select BALOP_10.i_dmid as i_dmid, BALOP_10.i_months_back as i_months_back, BALOP_10.c_offer_flag as c_offer_flag,  CASE WHEN (SUM(BALOP_10.i_sent) is null ) THEN (0) ELSE (SUM(BALOP_10.i_sent)) END as i_total_sent,  CASE WHEN (SUM(BALOP_10.i_total_opens) is null ) THEN (0) ELSE (SUM(BALOP_10.i_total_opens)) END as i_total_opens,  CASE WHEN (SUM(BALOP_10.i_total_clicks) is null ) THEN (0) ELSE (SUM(BALOP_10.i_total_clicks)) END as i_total_clicks 
from  
(select i_dmid as i_dmid, (((EXTRACT(YEAR FROM CURRENT_DATE) * 12) + EXTRACT(MONTH FROM CURRENT_DATE)) - ((EXTRACT(YEAR FROM d_eventdate) * 12) + EXTRACT(MONTH FROM d_eventdate))) - 1 as i_months_back,  CASE WHEN (c_offer_id is null ) THEN ('N') ELSE ('Y') END as c_offer_flag, i_sent as i_sent,  CASE WHEN (i_total_opens > 0) THEN (1) ELSE (0) END as i_total_opens,  CASE WHEN (i_total_clicks > 0) THEN (1) ELSE (0) END as i_total_clicks 
from  
(select BALOP_2.i_dmid as i_dmid, BALOP_8.d_eventdate as d_eventdate, BALOP_8.c_offer_id as c_offer_id, BALOP_8.i_sent as i_sent, BALOP_8.i_total_opens as i_total_opens, BALOP_8.i_total_clicks as i_total_clicks 
from  
(select CAST(i_dmid AS DECIMAL(11,0)) as i_dmid, CAST(i_xref_dmid AS DECIMAL(11,0)) as i_xref_dmid 
from #PS_DB_Name.$EDW_OS_EDWDB#.gst_cons_xref BALOP_1) BALOP_2 
inner join  
(select BALOP_7.i_xref_dmid as i_xref_dmid, BALOP_7.d_eventdate as d_eventdate, BALOP_7.c_offer_id as c_offer_id, BALOP_7.i_sent as i_sent, BALOP_7.i_total_opens as i_total_opens, BALOP_7.i_total_clicks as i_total_clicks 
from  
(select BALOP_4.i_xref_dmid as i_xref_dmid, BALOP_4.d_eventdate as d_eventdate, BALOP_4.c_offer_id as c_offer_id, BALOP_4.i_sent as i_sent, BALOP_4.i_total_opens as i_total_opens, BALOP_4.i_total_clicks as i_total_clicks, BALOP_6.d_min_rpt_period as d_min_rpt_period 
from  
(select CAST(i_xref_dmid AS DECIMAL(11,0)) as i_xref_dmid, CAST(CAST(d_eventdate AS DATE) AS DATE) as d_eventdate, CAST(c_offer_id AS CHAR(5)) as c_offer_id, CAST(1 AS INTEGER) as i_sent, CAST(i_total_opens AS INTEGER) as i_total_opens, CAST(i_total_clicks AS INTEGER) as i_total_clicks 
from #PS_DB_Name.$EDW_OS_EDWDB#.et_event_hist BALOP_3) BALOP_4 
inner join  
(select CAST(1 AS INTEGER) as i_sent, CAST(min(d_min_rpt_period) AS DATE) as d_min_rpt_period 
from #PS_DB_Name.$EDW_OS_PROCDB#.email_engage_measure_dates_proc BALOP_5) BALOP_6 
on (BALOP_4.i_sent = BALOP_6.i_sent)) BALOP_7 
where BALOP_7.d_eventdate >= BALOP_7.d_min_rpt_period) BALOP_8 
on (BALOP_2.i_xref_dmid = BALOP_8.i_xref_dmid)) BALOP_9) BALOP_10 
group by i_dmid, i_months_back, c_offer_flag) BALOP_11 
left outer join  
(select CAST(i_measure_id AS SMALLINT) as i_measure_id, CAST(c_offer_flag AS CHAR(1)) as c_offer_flag, CAST(c_action_cd AS CHAR(1)) as c_action_cd, CAST(i_months_back_from AS SMALLINT) as i_months_back_from, CAST(i_months_back_to AS SMALLINT) as i_months_back_to, CAST(f_measure_qty AS DECIMAL(9,2)) as f_measure_qty 
from #PS_DB_Name.$EDW_OS_EDWDB#.email_engage_measure BALOP_12 
where c_job_cd = 'MEASURE_COMPUTE') BALOP_13 
on (BALOP_11.c_offer_flag = BALOP_13.c_offer_flag)) BALOP_14 
where (BALOP_14.i_months_back >= BALOP_14.i_months_back_from) and (BALOP_14.i_months_back <= BALOP_14.i_months_back_to)) BALOP_15 
group by i_dmid, i_measure_id, c_action_cd, f_measure_qty) BALOP_16) BALOP_17;
