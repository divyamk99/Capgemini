--Step 1: Suspend task
ALTER TASK DAAS_COMMON.RAW2CORE_HETCMS_COMP_DETAIL_FACT_TASK SUSPEND;
ALTER TASK DAAS_COMMON.TRIP_ROOT_TASK SUSPEND;

--Step 2: Code check in & deploy below objects for Generic update
common/daas_common/ddl/procedure/core/comp_domain_generic_update_proc.sql,procedure
common/daas_common/ddl/task/comp_domain_generic_update_task.sql,task

--Step 3: Replay records from DAAS_RAW_HETCMS.CMPRW_RAW table
CALL DAAS_COMMON.DATA_REPLAY_PROC('DAAS_RAW_HETCMS', 'CMPRW_RAW', 
'WHERE DATE(OPERATION_DATE) <= ''2023-12-19''', 
'Data_masked at Core_bug', 10000000000); -- 177830195

--Step 4: Execute Raw2core task manually
EXECUTE TASK DAAS_COMMON.RAW2CORE_HETCMS_COMP_DETAIL_FACT_TASK;

--Step 5: Recreate streams created on top of Comp_Detail_Fact
CALL DAAS_COMMON.CREATE_OR_REPLACE_STREAM ('DAAS_CORE','COMP_DETAIL_FACT','DAAS_CORE','COMP_DETAIL_FACT_COMP_DOMAIN_STREAM',NULL);
CALL DAAS_COMMON.CREATE_OR_REPLACE_STREAM ('DAAS_CORE','COMP_DETAIL_FACT','DAAS_CORE','COMP_DETAIL_FACT_FP_DOMAIN_STREAM',NULL);

--Step 6: Run generic update for Comp to ensure none of the eligible records missed from fact to trips
EXECUTE TASK DAAS_COMMON.COMP_DOMAIN_GENERIC_UPDATE_TASK;

--Step 7: Execute TRIP_ROOT_TASK manually
EXECUTE TASK DAAS_COMMON.TRIP_ROOT_TASK;

--Step 8: Resume task
ALTER TASK DAAS_COMMON.TRIP_ROOT_TASK RESUME;
ALTER TASK DAAS_COMMON.RAW2CORE_HETCMS_COMP_DETAIL_FACT_TASK RESUME;