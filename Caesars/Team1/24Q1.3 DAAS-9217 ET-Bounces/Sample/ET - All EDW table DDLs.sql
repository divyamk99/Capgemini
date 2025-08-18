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

---------------

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


---------------


CREATE MULTISET TABLE EDW_MAIN.et_event_hist ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      i_clientid INTEGER COMPRESS ,
      i_sendid INTEGER NOT NULL,
      c_subscriber_key VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,
      i_xref_dmid DECIMAL(11,0),
      i_subscriberid INTEGER,
      c_email VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC COMPRESS '',
      i_listid INTEGER COMPRESS (40 ,47 ,52 ,55 ,56 ,57 ,58 ,59 ,60 ,61 ,62 ,63 ,64 ,65 ,66 ,67 ,68 ,69 ,70 ,71 ,72 ,73 ,74 ,75 ,76 ,77 ,78 ,79 ,80 ,81 ,82 ,83 ,84 ,85 ,86 ,87 ,88 ,100 ),
      d_eventdate TIMESTAMP(0) FORMAT 'yyyy-mm-ddbhh:mi:ssbt',
      c_eventtype CHAR(6) CHARACTER SET LATIN NOT CASESPECIFIC COMPRESS 'Sent  ',
      c_batchid VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,
      c_triggeredsendexternalkey VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC COMPRESS ('Nobu','ResConfirmCancel_Subscribers_Email','ResConfirmCancel_Subscribers_Email_O','Skyforce_ConfirmCancel','TR_Signup'),
      c_offer_id CHAR(5) CHARACTER SET LATIN NOT CASESPECIFIC COMPRESS '     ',
      c_fromemail VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC COMPRESS ('noreply@email.caesars-marketing.com','emails@email.caesars-marketing.com','chaynes1@caesars.com','email@email.caesars-marketing.com','Dynamic From Email'),
      c_subject VARCHAR(200) CHARACTER SET LATIN NOT CASESPECIFIC COMPRESS '',
      c_emailname VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC COMPRESS '',
      d_first_open_dt TIMESTAMP(0) FORMAT 'yyyy-mm-ddbhh:mi:ss' COMPRESS ,
      d_last_open_dt TIMESTAMP(0) FORMAT 'yyyy-mm-ddbhh:mi:ss' COMPRESS ,
      i_total_opens INTEGER COMPRESS (1 ,2 ,3 ,4 ,5 ,6 ),
      d_first_click_dt TIMESTAMP(0) FORMAT 'yyyy-mm-ddbhh:mi:ss' COMPRESS ,
      d_last_click_dt TIMESTAMP(0) FORMAT 'yyyy-mm-ddbhh:mi:ss' COMPRESS ,
      i_total_clicks INTEGER COMPRESS (1 ,2 ,3 ,4 ,5 ,6 ),
      i_total_bounces INTEGER COMPRESS (1 ,2 ,3 ,4 ,5 ,6 ),
      i_adj_total_bounce INTEGER COMPRESS (0 ,1 ),
      c_quality_cd CHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC DEFAULT ' ' COMPRESS ' ',
      d_timestamp TIMESTAMP(0) FORMAT 'yyyy-mm-ddbhh:mi:ss' NOT NULL DEFAULT CURRENT_TIMESTAMP(0))
PRIMARY INDEX ix_event_hist_1 ( i_xref_dmid );


---------------


CREATE MULTISET TABLE EDW_MAIN.ET_Sent_Event ,FALLBACK ,
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
      c_eventtype CHAR(6) CHARACTER SET LATIN NOT CASESPECIFIC COMPRESS 'Sent  ',
      c_batchid VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,
      c_triggeredsendexternalkey VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,
      c_offer_id CHAR(5) CHARACTER SET LATIN NOT CASESPECIFIC,
      c_quality_cd CHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC DEFAULT ' ' COMPRESS ' ',
      d_timestamp TIMESTAMP(0) FORMAT 'yyyy-mm-ddbhh:mi:ss' NOT NULL DEFAULT CURRENT_TIMESTAMP(0))
PRIMARY INDEX ( i_sendid ,i_dmid ,c_batchid );



---------------

CREATE MULTISET TABLE EDW_MAIN.ET_Clicks_Event ,FALLBACK ,
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
      d_eventdate TIMESTAMP(0) FORMAT 'MM/DD/YYYYbHH:MI:SS',
      c_eventtype CHAR(6) CHARACTER SET LATIN NOT CASESPECIFIC COMPRESS 'Click ',
      i_sendurlid INTEGER,
      i_urlid INTEGER,
      c_url VARCHAR(4000) CHARACTER SET LATIN NOT CASESPECIFIC,
      c_alias VARCHAR(1024) CHARACTER SET LATIN NOT CASESPECIFIC FORMAT 'X(500)',
      c_batchid VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,
      c_triggeredsendexternalkey VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,
      d_timestamp TIMESTAMP(0) FORMAT 'yyyy-mm-ddbhh:mi:ss' NOT NULL DEFAULT CURRENT_TIMESTAMP(0),
      c_quality_cd CHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC DEFAULT ' ' COMPRESS ' ')
PRIMARY INDEX ( i_sendid ,i_dmid ,c_batchid );



---------------

CREATE MULTISET TABLE EDW_MAIN.ET_Conversions ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      i_clientid INTEGER,
      i_sendid INTEGER NOT NULL,
      c_subscriber_key CHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
      i_dmid DECIMAL(11,0),
      c_emailaddress CHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,
      i_subscriberid INTEGER,
      i_listid INTEGER,
      d_eventdate TIMESTAMP(0) FORMAT 'yyyy-mm-ddbhh:mi:ss',
      c_eventtype CHAR(6) CHARACTER SET LATIN NOT CASESPECIFIC COMPRESS 'Conver',
      c_referringurl VARCHAR(8000) CHARACTER SET LATIN NOT CASESPECIFIC,
      c_linkalias VARCHAR(1024) CHARACTER SET LATIN NOT CASESPECIFIC FORMAT 'X(500)',
      c_conversiondata VARCHAR(8000) CHARACTER SET LATIN NOT CASESPECIFIC,
      c_batchid VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,
      c_triggeredsendexternalkey VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,
      i_urlid INTEGER,
      d_timestamp TIMESTAMP(0) FORMAT 'yyyy-mm-ddbhh:mi:ss' NOT NULL DEFAULT CURRENT_TIMESTAMP(0),
      c_quality_cd CHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC DEFAULT ' ' COMPRESS ' ')
PRIMARY INDEX ( c_subscriber_key );



---------------

CREATE MULTISET TABLE EDW_MAIN.ET_Bounces_Event ,FALLBACK ,
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
      d_eventdate TIMESTAMP(0) FORMAT 'yyyy-mm-ddbhh:mi:ss',
      c_eventtype CHAR(6) CHARACTER SET LATIN NOT CASESPECIFIC COMPRESS 'Bounce',
      c_bounce_category VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,
      i_smtp_code INTEGER,
      c_bounce_reason VARCHAR(500) CHARACTER SET LATIN NOT CASESPECIFIC,
      c_batchid VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
      c_triggeredsendexternalkey VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,
      c_quality_cd CHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC DEFAULT ' ' COMPRESS ' ',
      d_timestamp TIMESTAMP(0) FORMAT 'yyyy-mm-ddbhh:mi:ss' NOT NULL DEFAULT CURRENT_TIMESTAMP(0))
PRIMARY INDEX ( i_sendid ,i_dmid ,c_batchid );