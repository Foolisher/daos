

CREATE KEYSPACE groupon WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'datacenter1' : 2 };

CREATE TABLE if not exists  groupon.groupon_summary_references (
  sum_for     timestamp,
  type        int,
  total       int,
  registered  int,
  activated   int,
  PRIMARY KEY (sum_for, type)
);


CREATE TABLE if not exists groupon.groupon_summary_deals (
  sum_for     timestamp,
  gmv         bigint,
  gross_order INT ,
  gross_item  INT ,
  deal        bigint,
  per_order   INT ,
  deal_order  bigint,
  deal_item   INT ,
  PRIMARY KEY (sum_for)
);


CREATE TABLE groupon.groupon_summary_users (
    year           int,
    month          int,
    date           int,
    net_increase   bigint,
    total          bigint,
    channel        int,
    PRIMARY KEY (year, month, date)
  ) WITH CLUSTERING ORDER BY (month DESC, date DESC);
