1. Collect descripancy GUID from trip_summary_vw for prop vs ent --> Replay from fact
2. Collect descripancy GUID from trip_summary_vw for mark vs ent --> Replay from fact
3. Collect descripancy GUID from daily_activity_summary_vw for prop vs ent --> Replay from fact
4. Collect descripancy GUID from daily_activity_summary_vw for mark vs ent --> Replay from fact


--If Records not fixed in above:
Collect all GUID's into temp table  
Delete from trip tables
Replay from fact