

CREATE KEYSPACE groupon WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'datacenter1' : 2 };

DROP TABLE groupon.groupon_summary_references;
DROP TABLE groupon.groupon_summary_deals;
DROP TABLE groupon.groupon_summary_users;

CREATE TABLE if not exists  groupon.groupon_summary_references (
    year_month  int,
    day         int,
    registered  int,
    activated   int,
    total       int,
    type        int,
    PRIMARY KEY ((year_month), day)
  ) WITH CLUSTERING ORDER BY (day DESC);

  CREATE TABLE if not exists groupon.groupon_summary_deals (
    year_month  int,
    day         int,
    type        text,
    gmv         bigint,
    gross_order INT ,
    gross_item  INT ,
    deal        bigint,
    per_order   INT ,
    deal_order  bigint,
    deal_item   INT ,
    PRIMARY KEY ((year_month), day)
  ) WITH CLUSTERING ORDER BY (day DESC);

  CREATE TABLE groupon.groupon_summary_users (
    year_month     int,
    day            int,
    channel        text,
    net_increase   bigint,
    total          bigint,
    PRIMARY KEY ((year_month), day)
  ) WITH CLUSTERING ORDER BY (day DESC);


