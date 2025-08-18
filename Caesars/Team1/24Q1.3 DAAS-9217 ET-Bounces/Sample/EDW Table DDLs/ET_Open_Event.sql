CREATE MULTISET TABLE EDW_MAIN.ET_Open_Event ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      i_clientid INTEGER,
      i_sendid INTEGER NOT NULL,
      c_subscriber_key VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
      i_dmid DECIMAL(11,0),
      c_emailaddress VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,
      i_subscriberid INTEGER,
      i_listid INTEGER,
      d_eventdate TIMESTAMP(0) FORMAT 'yyyy-mm-ddbhh:mi:ssbt',
      c_eventtype CHAR(6) CHARACTER SET LATIN NOT CASESPECIFIC COMPRESS 'Open  ',
      c_batchid VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,
      c_triggeredsendexternalkey VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,
      d_timestamp TIMESTAMP(0) FORMAT 'yyyy-mm-ddbhh:mi:ss' NOT NULL DEFAULT CURRENT_TIMESTAMP(0),
      c_quality_cd CHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC DEFAULT ' ' COMPRESS ' ')
PRIMARY INDEX ( i_sendid ,i_dmid ,c_batchid );