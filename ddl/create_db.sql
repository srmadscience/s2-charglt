CREATE DATABASE charglt;
 
USE charglt;

CREATE rowstore table user_table
(userid bigint not null primary key
,user_json_object varchar(8000)
,user_last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
,user_softlock_sessionid bigint 
,user_softlock_expiry TIMESTAMP
,user_balance bigint not null);

create index ut_del on user_table(user_last_seen);

-- create index ut_loyaltycard on user_table (field(user_json_object, 'loyaltySchemeNumber'));

create table user_usage_table
(userid bigint not null
,allocated_amount bigint not null
,sessionid bigint  not null
,lastdate timestamp not null
,primary key (userid, sessionid)
,shard key(userid));


-- USING TTL 180 MINUTES ON COLUMN lastdate;

CREATE INDEX ust_del_idx1 ON user_usage_table(lastdate);

CREATE INDEX uut_ix1 ON user_usage_table(userid, lastdate);

create table user_recent_transactions
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

--CREATE INDEX urt_del_idx2 ON user_recent_transactions(userid, txn_time)  WHERE NOT MIGRATING;

CREATE INDEX urt_del_idx3 ON user_recent_transactions(txn_time);

--CREATE INDEX urt_del_idx4 ON user_recent_transactions(txn_time) WHERE NOT MIGRATING;

--CREATE STREAM user_financial_events 
--EXPORT TO TOPIC user_financial_events 
--WITH KEY (userid)
--partition on column userid
--(userid bigint not null 
--,amount bigint not null
--,user_txn_id varchar(128) not null
--,message varchar(80) not null);

create view current_locks as
select count(*) how_many 
from user_table 
where user_softlock_expiry is not null;

create view user_balance as
select userid, sum(amount) balance
from user_financial_events
group by userid;

create view allocated_credit as 
select sum(allocated_amount) allocated_amount 
from user_usage_table;

create view users_sessions as 
select userid, count(*) how_many 
from user_usage_table
group by  userid;

-- create index uss_ix1 on users_sessions (how_many) WHERE how_many > 1;

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

-- create procedure GetUsersWithMultipleSessions
-- AS 
-- SELECT * FROM users_sessions 
-- WHERE how_many > 1
-- ORDER BY how_many, userid, sessionid
-- LIMIT 50;
-- 

DELIMITER //

create or replace procedure GetUsersWithMultipleSessions()
AS 
BEGIN
ECHO SELECT * FROM users_sessions
WHERE how_many > 1
ORDER BY how_many, userid
LIMIT 50;
END//

-- create procedure showTransactions
-- PARTITION ON TABLE user_table COLUMN userid
-- as 
-- select * from user_recent_transactions where userid = ? ORDER BY txn_time, user_txn_id;
-- 

create procedure showTransactions(p_userid bigint)
as 
BEGIN
echo select * from user_recent_transactions where userid = p_userid ORDER BY txn_time, user_txn_id;
END;


DELIMITER ;
-- create procedure FindByLoyaltyCard as select * from user_table where field(user_json_object, 'loyaltySchemeNumber') = CAST(? AS VARCHAR);
-- 
-- CREATE PROCEDURE ShowCurrentAllocations__promBL AS
-- BEGIN
-- select 'user_locks' statname,  'user_locks' stathelp  ,how_many statvalue from current_locks;
-- select 'user_count' statname,  'user_count' stathelp  ,how_many statvalue from cluster_users;
-- select 'allocated_credit' statname,  'allocated_credit' stathelp  ,allocated_amount statvalue from allocated_credit;
-- select 'recent_activity_out_approved' statname
--      , 'recent_activity_out_approved' stathelp  
--      , approved_amount statvalue 
-- from recent_activity_out where txn_time = date_trunc('minute', DATEADD('minute', -1, NOW));
-- select 'recent_activity_out_spent' statname
--      , 'recent_activity_out_spent' stathelp  
--      , spent_amount statvalue 
-- from recent_activity_out where txn_time = date_trunc('minute', DATEADD('minute', -1, NOW));
-- select 'recent_activity_out_qty' statname
--      , 'recent_activity_out_qty' stathelp  
--      , how_many statvalue 
-- from recent_activity_out where txn_time = date_trunc('minute', DATEADD('minute', -1, NOW));
-- select 'recent_activity_in_spent' statname
--      , 'recent_activity_in_spent' stathelp  
--      , spent_amount statvalue 
-- from recent_activity_in where txn_time = date_trunc('minute', DATEADD('minute', -1, NOW));
-- select 'recent_activity_in_qty' statname
--      , 'recent_activity_in_qty' stathelp  
--      , how_many statvalue 
-- from recent_activity_in where txn_time = date_trunc('minute', DATEADD('minute', -1, NOW));
-- END;
-- 
-- 
-- CREATE PROCEDURE 
--    PARTITION ON TABLE user_table COLUMN userid
--    FROM CLASS chargingdemoprocs.GetUser;
--    

CREATE OR REPLACE PROCEDURE GetUser(p_userid bigint) 
AS
BEGIN
ECHO SELECT * FROM user_table WHERE userid = p_userid;
ECHO SELECT * FROM user_usage_table WHERE userid = p_userid ORDER BY sessionid;
ECHO SELECT * FROM user_recent_transactions WHERE userid = p_userid ORDER BY txn_time, user_txn_id;
END;

-- CREATE PROCEDURE 
--    PARTITION ON TABLE user_table COLUMN userid
--    FROM CLASS chargingdemoprocs.GetAndLockUser;


CREATE OR REPLACE PROCEDURE GetAndLockUser(p_userid bigint, p_new_lock_id bigint)
AS
DECLARE
--
  l_status_byte int := 42; 
  l_status_string text := 'OK';
--
  q_user QUERY(userid BIGINT,user_softlock_expiry TIMESTAMP, user_softlock_sessionid bigint) = 
     SELECT userid, user_softlock_expiry, user_softlock_sessionid FROM user_table WHERE userid = p_userid;
--
  l_found_userid bigint := null;
  l_user_softlock_expiry timestamp := null;
  l_user_softlock_sessionid bigint := null;
--
BEGIN
--
  FOR x IN COLLECT(q_user) LOOP
--
    l_found_userid := x.userid;
    l_user_softlock_expiry := x.user_softlock_expiry;
    l_user_softlock_sessionid := x.user_softlock_sessionid;
--
  END LOOP;
--
  IF l_found_userid = p_userid THEN
--
    IF l_user_softlock_sessionid = p_new_lock_id OR l_user_softlock_expiry IS NULL OR l_user_softlock_expiry < NOW(6) THEN
--
--  Take lock...
--
      UPDATE user_table SET user_softlock_sessionid = p_new_lock_id, user_softlock_expiry = DATE_ADD(NOW(), INTERVAL 1 SECOND)  WHERE userid = p_userid;
      l_status_byte = 54; 
      l_status_string = 'User ' || p_userid || ' locked by session '||l_user_softlock_sessionid;
-- 
    ELSE
-- 
--    Record is locked - STATUS_RECORD_ALREADY_SOFTLOCKED
--
      l_status_byte = 53; 
      l_status_string = 'User ' || p_userid || ' already locked by session '||l_user_softlock_sessionid;
--
    END IF;
--
  ELSE
--
-- No user found. Set STATUS_USER_DOESNT_EXIST
--
    l_status_byte = 50; 
    l_status_string = 'User ' || p_userid || ' does not exist';
--
  END IF;
--
ECHO SELECT * FROM user_table WHERE userid = p_userid;
ECHO SELECT user_txn_id, txn_time FROM user_recent_transactions WHERE userid = p_userid ORDER BY txn_time, user_txn_id;
ECHO SELECT * FROM user_usage_table WHERE userid = p_userid ORDER BY sessionid;
echo SELECT l_status_byte, l_status_string from dual;
END;


-- CREATE PROCEDURE 
--    PARTITION ON TABLE user_table COLUMN userid
--    FROM CLASS chargingdemoprocs.UpdateLockedUser;
--    
-- CREATE PROCEDURE 
--    PARTITION ON TABLE user_table COLUMN userid
--    FROM CLASS chargingdemoprocs.UpsertUser;
--    
-- CREATE PROCEDURE 
--    PARTITION ON TABLE user_table COLUMN userid
--    FROM CLASS chargingdemoprocs.DelUser;

CREATE OR REPLACE PROCEDURE DelUser (p_userid bigint) AS
BEGIN
DELETE FROM user_table WHERE userid = p_userid;
DELETE FROM user_usage_table WHERE userid = p_userid;
DELETE FROM user_recent_transactions WHERE userid = p_userid;
END;


--    
-- CREATE PROCEDURE 
--    PARTITION ON TABLE user_table COLUMN userid
--    FROM CLASS chargingdemoprocs.ReportQuotaUsage;  
--    
-- CREATE PROCEDURE 
--    PARTITION ON TABLE user_table COLUMN userid
--    FROM CLASS chargingdemoprocs.AddCredit;  
-- 
-- 
-- END_OF_BATCH
