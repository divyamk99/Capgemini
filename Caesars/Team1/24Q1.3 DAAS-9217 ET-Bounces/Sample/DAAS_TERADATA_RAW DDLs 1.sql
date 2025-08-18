CREATE TABLE DAAS_TERADATA_RAW.ET_Open_Event 
(
      i_clientid INTEGER,
      i_sendid INTEGER NOT NULL,
      c_subscriber_key VARCHAR(100) NOT NULL,
      i_dmid DECIMAL(11,0),
      c_emailaddress VARCHAR(100) ,
      i_subscriberid INTEGER,
      i_listid INTEGER,
      d_eventdate TIMESTAMP(9),
      c_eventtype VARCHAR(6) ,
      c_batchid VARCHAR(100) ,
      c_triggeredsendexternalkey VARCHAR(100) ,
      d_timestamp TIMESTAMP(9) DEFAULT CURRENT_TIMESTAMP() NOT NULL,
      c_quality_cd VARCHAR(1) DEFAULT ' ',
;

---------------

CREATE  TABLE DAAS_TERADATA_RAW.ET_Send_Jobs 
(
      i_clientid INTEGER,
      i_sendid INTEGER NOT NULL,
      c_fromname VARCHAR(130) ,
      c_fromemail VARCHAR(100) ,
      d_schedtime TIMESTAMP(9),
      d_senttime TIMESTAMP(9),
      c_subject VARCHAR(200) ,
      c_emailname VARCHAR(100) ,
      c_triggeredsendexternalkey VARCHAR(100) ,
      c_senddefinitionexternalkey VARCHAR(100) ,
      c_jobstatus VARCHAR(30) ,
      c_previewurl VARCHAR(300) ,
      c_additional VARCHAR(50) ,
      d_timestamp TIMESTAMP(9) DEFAULT CURRENT_TIMESTAMP() NOT NULL,
      c_quality_cd VARCHAR(1) DEFAULT ' ',
      c_ismultipart VARCHAR(20) )
;



---------------


CREATE  TABLE DAAS_TERADATA_RAW.ET_Sent_Event 
(
      i_clientid INTEGER,
      i_sendid INTEGER NOT NULL,
      c_subscriber_key VARCHAR(100) NOT NULL,
      i_dmid DECIMAL(11,0),
      c_emailaddress VARCHAR(100) ,
      i_subscriberid INTEGER,
      i_listid INTEGER,
      d_eventdate TIMESTAMP(9),
      c_eventtype VARCHAR(6) ,
      c_batchid VARCHAR(100) ,
      c_triggeredsendexternalkey VARCHAR(100) ,
      c_offer_id VARCHAR(5) ,
      c_quality_cd VARCHAR(1) DEFAULT ' ',
      d_timestamp TIMESTAMP(9) NOT NULL DEFAULT CURRENT_TIMESTAMP())
;



---------------

CREATE  TABLE DAAS_TERADATA_RAW.ET_Clicks_Event 
(
      i_clientid INTEGER,
      i_sendid INTEGER NOT NULL,
      c_subscriber_key VARCHAR(100) NOT NULL,
      i_dmid DECIMAL(11,0),
      c_emailaddress VARCHAR(100) ,
      i_subscriberid INTEGER,
      i_listid INTEGER,
      d_eventdate TIMESTAMP(9),
      c_eventtype VARCHAR(6) ,
      i_sendurlid INTEGER,
      i_urlid INTEGER,
      c_url VARCHAR(4000) ,
      c_alias VARCHAR(1024),
      c_batchid VARCHAR(100) ,
      c_triggeredsendexternalkey VARCHAR(100) ,
      d_timestamp TIMESTAMP(9) DEFAULT CURRENT_TIMESTAMP() NOT NULL,
      c_quality_cd VARCHAR(1) DEFAULT ' ',
;



---------------

CREATE  TABLE DAAS_TERADATA_RAW.ET_Conversions 
(
      i_clientid INTEGER,
      i_sendid INTEGER NOT NULL,
      c_subscriber_key VARCHAR(100) NOT NULL,
      i_dmid DECIMAL(11,0),
      c_emailaddress VARCHAR(100) ,
      i_subscriberid INTEGER,
      i_listid INTEGER,
      d_eventdate TIMESTAMP(9),
      c_eventtype VARCHAR(6) ,
      c_referringurl VARCHAR(8000) ,
      c_linkalias VARCHAR(1024),
      c_conversiondata VARCHAR(8000) ,
      c_batchid VARCHAR(100) ,
      c_triggeredsendexternalkey VARCHAR(100) ,
      i_urlid INTEGER,
      d_timestamp TIMESTAMP(9) DEFAULT CURRENT_TIMESTAMP() NOT NULL,
      c_quality_cd VARCHAR(1) DEFAULT ' ',
;



---------------

CREATE  TABLE DAAS_TERADATA_RAW.ET_Bounces_Event 
(
      i_clientid INTEGER,
      i_sendid INTEGER NOT NULL,
      c_subscriber_key VARCHAR(100) NOT NULL,
      i_dmid DECIMAL(11,0),
      c_emailaddress VARCHAR(100) ,
      i_subscriberid INTEGER,
      i_listid INTEGER,
      d_eventdate TIMESTAMP(9),
      c_eventtype VARCHAR(6) ,
      c_bounce_category VARCHAR(100) ,
      i_smtp_code INTEGER,
      c_bounce_reason VARCHAR(500) ,
      c_batchid VARCHAR(100) NOT NULL,
      c_triggeredsendexternalkey VARCHAR(100) ,
      c_quality_cd VARCHAR(1) DEFAULT ' ',
      d_timestamp TIMESTAMP(9)  NOT NULL DEFAULT CURRENT_TIMESTAMP())
;