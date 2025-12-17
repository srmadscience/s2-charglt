 
create database charglt;


USE charglt;

CREATE rowstore table user_table
(userid bigint not null primary key
,user_last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
,user_softlock_sessionid bigint 
,user_softlock_expiry TIMESTAMP
,user_balance bigint not null
,user_json_object JSON
,user_json_cardid AS user_json_object::$loyaltySchemeNumber PERSISTED LONGTEXT
,shard key (userid));

create index ut_del on user_table(user_last_seen);

create index ut_loyaltycard on user_table (user_json_cardid);

create rowstore table user_usage_table
(userid bigint not null
,allocated_amount bigint not null
,sessionid bigint  not null
,lastdate timestamp not null
,primary key (userid, sessionid)
,shard key(userid));


CREATE INDEX ust_del_idx1 ON user_usage_table(lastdate);

CREATE INDEX uut_ix1 ON user_usage_table(userid, lastdate);

create rowstore table user_recent_transactions
(userid bigint not null 
,user_txn_id varchar(128) NOT NULL
,txn_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP not null 
,sessionid bigint
,approved_amount bigint 
,spent_amount bigint 
,purpose  varchar(128)
,primary key (userid, user_txn_id)
,shard key(userid));


CREATE INDEX urt_del_idx ON user_recent_transactions(userid, txn_time,user_txn_id) ;

CREATE INDEX urt_del_idx3 ON user_recent_transactions(txn_time);


create view current_locks as
select count(*) how_many 
from user_table 
where user_softlock_expiry is not null;

create view allocated_credit as 
select sum(allocated_amount) allocated_amount 
from user_usage_table;

create view users_sessions as 
select userid, count(*) how_many 
from user_usage_table
group by  userid;


create view recent_activity_out as
select DATE_TRUNC('minute',txn_time) txn_time
       , sum(approved_amount * -1) approved_amount
       , sum(spent_amount) spent_amount
       , count(*) how_many
from user_recent_transactions
where spent_amount <= 0
GROUP BY DATE_TRUNC('minute',txn_time) ;

create view recent_activity_in as
select DATE_TRUNC('minute',txn_time) txn_time
       , sum(approved_amount) approved_amount
       , sum(spent_amount) spent_amount
       , count(*) how_many
from user_recent_transactions
where spent_amount > 0
GROUP BY DATE_TRUNC('minute',txn_time) ;


create view cluster_activity_by_users as 
select userid,  count(*) how_many
from user_recent_transactions
group by userid;

create view cluster_activity as 
select date_trunc('minute', txn_time) txn_time, count(*) how_many
from user_recent_transactions
group by date_trunc('minute', txn_time) ;

create view last_cluster_activity as 
select  max(txn_time) txn_time
from user_recent_transactions;

create view cluster_users as 
select  count(*) how_many
from user_table;


