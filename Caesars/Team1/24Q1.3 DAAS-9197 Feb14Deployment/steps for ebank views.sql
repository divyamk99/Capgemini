Step 1:- Deploy the below table,views,task and procedure through PROD Jenkins standard pipeline

raw/daas_raw_ebank/ddl/table/eb_deposit_withdraw_raw.sql,table
raw/daas_raw_ebank/ddl/view/eb_activity_type_vw.sql,view
raw/daas_raw_ebank/ddl/view/eb_earning_type_vw.sql,view
raw/daas_raw_ebank/ddl/view/eb_fund_type_vw.sql,view
raw/daas_raw_ebank/ddl/view/eb_gaming_hospitality_vw.sql,view
raw/daas_raw_ebank/ddl/view/eb_source_data_element_vw.sql,view
raw/daas_raw_ebank/ddl/view/eb_transaction_type_vw.sql,view
raw/daas_raw_ebank/ddl/view/eb_deposit_withdraw_vw.sql,view
common/daas_common/ddl/procedure/core/eb_deposit_withdraw_raw_load.sql,procedure
common/daas_common/ddl/task/eb_deposit_withdraw_raw_load_task.sql,task

Step 2:- Execute task to insert data into DAAS_TERADATA_RAW.EB_DEPOSIT_WITHDRAW_RAW

EXECUTE TASK DAAS_COMMON.EB_DEPOSIT_WITHDRAW_RAW_LOAD_TASK;

Step 3:- Validate all views

SELECT * FROM DAAS_RAW_EBANK_VW.EB_ACTIVITY_TYPE_VW;
SELECT * FROM DAAS_RAW_EBANK_VW.EB_EARNING_TYPE_VW;
SELECT * FROM DAAS_RAW_EBANK_VW.EB_FUND_TYPE_VW;
SELECT * FROM DAAS_RAW_EBANK_VW.EB_GAMING_HOSPITALITY_VW;
SELECT * FROM DAAS_RAW_EBANK_VW.EB_SOURCE_DATA_ELEMENT_VW;
SELECT * FROM DAAS_RAW_EBANK_VW.EB_TRANSACTION_TYPE_VW;
SELECT * FROM DAAS_RAW_EBANK_VW.EB_DEPOSIT_WITHDRAW_VW;