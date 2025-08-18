CREATE PIPE IF NOT EXISTS DAAS_COMMON.STG_ET_CLICKS_EVENT_DAAS_SNOWPIPE AUTO_INGEST = TRUE
AS
COPY INTO DAAS_EXACT_TARGET_STG.ET_CLICKS_EVENT_STG
(
	i_clientid,
	i_sendid,
	c_subscriber_key,
	c_emailaddress,
	i_subscriberid,
	i_listid,
	d_eventdate,
	c_eventtype,
	i_sendurlid,
	i_urlid,
	c_url,
	c_alias,
	c_batchid,
	c_triggeredsendexternalkey,
	SOURCE_SYSTEM_NAME,
	TIME_ZONE,
	CREATED_DATE,
	CREATED_BY,
	UPDATED_DATE,
	UPDATED_BY,
	REPLAY_COUNTER,
	SOURCE_FILE_NAME
)
FROM
(SELECT 
	$1,
	$2,
	$3,
	$4,
	$5,
	$6,
	$7,
	$8,
	$9,
	$10,
	$11,
	$12,
	$13,
	$14,
	'EXACT TARGET',
	'US/CENTRAL',
	CURRENT_TIMESTAMP,
	CURRENT_USER,
	CURRENT_TIMESTAMP,
	CURRENT_USER,
	0,
	METADATA$FILENAME
FROM @DAAS_COMMON.DAAS_EXT_STG_<>/<>/out/<> )
--(FILE_FORMAT => DAAS_COMMON.DAAS_CSV_FF_WITHOUT_HEADER ) 
file_format = (type = csv field_delimiter = '|' skip_header = 1 escape = '\134' encoding = 'iso-8859-1') 
;