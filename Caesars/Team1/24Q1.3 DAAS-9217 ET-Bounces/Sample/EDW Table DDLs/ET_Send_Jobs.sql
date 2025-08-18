CREATE MULTISET TABLE EDW_MAIN.ET_Send_Jobs ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      i_clientid INTEGER,
      i_sendid INTEGER NOT NULL,
      c_fromname VARCHAR(130) CHARACTER SET LATIN NOT CASESPECIFIC,
      c_fromemail VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,
      d_schedtime TIMESTAMP(0) FORMAT 'yyyy-mm-ddbhh:mi:ssbt',
      d_senttime TIMESTAMP(0) FORMAT 'yyyy-mm-ddbhh:mi:ssbt',
      c_subject VARCHAR(200) CHARACTER SET LATIN NOT CASESPECIFIC,
      c_emailname CHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,
      c_triggeredsendexternalkey CHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,
      c_senddefinitionexternalkey CHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,
      c_jobstatus CHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC COMPRESS ('Canceled                      ','Complete                      ','Deleted                       ','Error                         ','New                           ','PostSendCallout               ','Scheduled                     ','Sending                       ','Stopped                       '),
      c_previewurl VARCHAR(300) CHARACTER SET LATIN NOT CASESPECIFIC,
      c_additional CHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
      d_timestamp TIMESTAMP(0) FORMAT 'yyyy-mm-ddbhh:mi:ss' NOT NULL DEFAULT CURRENT_TIMESTAMP(0),
      c_quality_cd CHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC DEFAULT ' ' COMPRESS ' ',
      c_ismultipart VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC)
PRIMARY INDEX ( i_sendid );