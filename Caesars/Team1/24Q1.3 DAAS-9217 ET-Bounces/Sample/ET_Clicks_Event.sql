CREATE TABLE IF NOT EXISTS DAAS_TERADATA_RAW.ET_Clicks_Event
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

ALTER TABLE IF EXISTS DAAS_TERADATA_RAW.ET_Clicks_Event ADD SEARCH OPTIMIZATION ON EQUALITY(i_sendid);
ALTER TABLE IF EXISTS DAAS_TERADATA_RAW.ET_Clicks_Event ADD SEARCH OPTIMIZATION ON EQUALITY(i_dmid);
ALTER TABLE IF EXISTS DAAS_TERADATA_RAW.ET_Clicks_Event ADD SEARCH OPTIMIZATION ON EQUALITY(c_batchid);