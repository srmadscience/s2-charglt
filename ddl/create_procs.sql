DELIMITER //

USE charglt; 

create or replace procedure GetUsersWithMultipleSessions()
AS 
BEGIN
ECHO SELECT * FROM users_sessions
WHERE how_many > 1
ORDER BY how_many, userid
LIMIT 50;
END//


create or replace procedure showTransactions(p_userid bigint)
as 
BEGIN
echo select * from user_recent_transactions where userid = p_userid ORDER BY txn_time, user_txn_id;
END//



CREATE OR REPLACE PROCEDURE GetUser(p_userid bigint) 
AS
BEGIN
ECHO SELECT * FROM user_table WHERE userid = p_userid;
ECHO SELECT * FROM user_usage_table WHERE userid = p_userid ORDER BY sessionid;
ECHO SELECT * FROM user_recent_transactions WHERE userid = p_userid ORDER BY txn_time, user_txn_id;
END//


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
END//

CREATE OR REPLACE PROCEDURE UpdateLockedUser(p_userid bigint, p_new_lock_id bigint, p_json_payload TEXT, p_delta_operation_name TEXT)
AS
DECLARE
--
  l_status_byte int := 0; 
  l_status_string text := '';
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
--  Update. Note we havem't implemented deltas yet
--
      UPDATE user_table SET user_softlock_sessionid = NULL, user_softlock_expiry = NULL, user_json_object = p_json_payload WHERE userid = p_userid;   
      l_status_byte = 42; 
      l_status_string = 'User ' || p_userid || ' updated';
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
END//


CREATE OR REPLACE PROCEDURE UpsertUser(p_userid BIGINT, p_addBalance BIGINT, p_json TEXT, p_purpose TEXT, p_lastSeen TIMESTAMP,
            p_txnId TEXT) AS
--
DECLARE
  l_status_byte int := 0;
  l_status_string text := '';
--
  q_user QUERY(userid BIGINT,user_softlock_expiry TIMESTAMP, user_softlock_sessionid bigint) =
     SELECT userid, user_softlock_expiry, user_softlock_sessionid FROM user_table WHERE userid = p_userid;
--
  q_txn QUERY(user_txn_id BIGINT) =
     SELECT user_txn_id FROM user_recent_transactions WHERE userid = p_userid AND user_txn_id = p_txnId;
--
  l_found_userid bigint := null;
  l_user_softlock_expiry timestamp := null;
  l_user_softlock_sessionid bigint := null;
--
  l_found_txn_id bigint := null;
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
  FOR y in COLLECT(q_txn) LOOP
--
    l_found_txn_id := y.user_txn_id;
--
  END LOOP;
--
  IF l_found_txn_id IS NOT NULL AND l_found_txn_id = p_txnId THEN
--
-- Txn has already happened
--
    l_status_byte = 46;
    l_status_string = 'Txn ' || p_txnId || ' already happened';
--
  ELSE
--
    IF l_found_userid = p_userid THEN
--
--  Update
--
      UPDATE user_table 
      SET user_json_object = p_json, user_last_seen = p_lastSeen
        , user_balance = p_addBalance, user_softlock_expiry = null, user_softlock_sessionid = null
      WHERE userid = p_userid;
--
      l_status_string = 'User ' || p_userid || ' updated';
--
    ELSE
--
-- Insert
--
      INSERT INTO user_table (userid, user_json_object,user_last_seen,user_balance) VALUES (p_userid,p_json,p_lastSeen,p_addBalance);
--
      l_status_string = 'User ' || p_userid || ' inserted';
--
    END IF;
--
    l_status_byte = 42;
--
    INSERT INTO user_recent_transactions (userid, user_txn_id, txn_time, approved_amount,spent_amount,purpose) VALUES (p_userid,p_txnId,p_lastSeen,0,p_addBalance,'Create User');
--
  END IF;
--
  echo SELECT l_status_byte, l_status_string from dual;
END//

CREATE OR REPLACE PROCEDURE DelUser (p_userid bigint) AS
BEGIN
DELETE FROM user_table WHERE userid = p_userid;
DELETE FROM user_usage_table WHERE userid = p_userid;
DELETE FROM user_recent_transactions WHERE userid = p_userid;
END//

CREATE OR REPLACE PROCEDURE ReportQuotaUsage(p_userid bigint, p_units_used bigint , p_units_wanted bigint , p_sessionid bigint, p_txnId TEXT)
AS
DECLARE
--
  l_status_byte int := 0;
  l_status_string text := ''; 
--
  q_user QUERY(userid BIGINT, user_balance BIGINT) =
     SELECT userid, user_balance FROM user_table WHERE userid = p_userid;
--
  q_txn QUERY(user_txn_id BIGINT) =
     SELECT user_txn_id FROM user_recent_transactions WHERE userid = p_userid AND user_txn_id = p_txnId;
--
  q_allocated QUERY(allocated_amount BIGINT) =
     SELECT nvl(sum(allocated_amount),0) allocated_amount FROM user_usage_table WHERE userid = p_userid AND sessionid != p_sessionid;
--
  l_found_userid bigint := null;
  l_found_txn_id bigint := null;
--
  l_balance bigint;
  l_allocated_amount bigint := 0;
  l_amount_spent bigint;
  l_decision text;
  l_available_credit bigint := 0;
  l_offered_credit bigint := 0;
--
BEGIN
--
  l_amount_spent := p_units_used * -1;
  l_decision := 'Spent '||l_amount_spent;
--
  FOR x IN COLLECT(q_user) LOOP
--
    l_found_userid := x.userid;
    l_balance := x.user_balance;
--
  END LOOP;
--
  FOR y IN COLLECT(q_txn) LOOP
--
    l_found_txn_id := y.user_txn_id;
--
  END LOOP;
--
  FOR z IN COLLECT(q_allocated) LOOP
--
    l_allocated_amount := l_allocated_amount + z.allocated_amount;
--
  END LOOP;
--
  IF l_found_txn_id = p_txnId THEN
--
-- Txn has already happened
--
    l_status_byte = 46; 
    l_status_string = 'Txn ' || p_txnId || ' already happened';
--
  ELSE
--
    IF l_found_userid = p_userid THEN
--
--    Get rid of old session record - we now have up to date usage info...
--
      DELETE FROM user_usage_table WHERE userid = p_userid AND sessionid = p_sessionid;
--
--    Calculate available credit - balance minus stuff promised to other sessions...
--
      l_available_credit := l_balance + l_amount_spent - l_allocated_amount;
--
      IF l_available_credit < 0 THEN
-- 
        l_status_byte = 43; 
        l_status_string = 'Negative balance: ' || l_available_credit; 
--
      ELSE
--
        IF p_units_wanted > l_available_credit THEN
--   
          l_offered_credit := l_available_credit;
--
--        Some units asked for allocated
--
          l_status_byte = 44; 
          l_status_string = l_offered_credit || ' of '|| p_units_wanted || ' Allocated';
--
        ELSE
--
          l_offered_credit := p_units_wanted;
--
--        All units asked for allocated
--
          l_status_byte = 45; 
          l_status_string = l_offered_credit || ' Allocated';
--
        END IF;
--      
        INSERT INTO user_recent_transactions  
          (userid, user_txn_id, txn_time, approved_amount,spent_amount,purpose) 
        VALUES 
          (p_userid,p_txnId,NOW(),l_offered_credit,l_amount_spent,'Spend');  
--
      END IF;
--
      UPDATE user_table 
      SET user_balance = user_balance + l_amount_spent
      WHERE userid = p_userid;
--
--    Create new session record if needed...
--
      IF l_offered_credit > 0 THEN
--
        INSERT INTO user_usage_table 
        (userid, allocated_amount,sessionid, lastdate) 
        VALUES 
        (p_userid,l_offered_credit,p_sessionid,NOW());
--
      END IF;
--
--    Delete old TX records
--
      DELETE FROM user_recent_transactions 
      WHERE userid = p_userid
      AND txn_time < DATE_ADD(NOW(), INTERVAL -1 SECOND);
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
  END IF; 
--
ECHO SELECT * FROM user_table WHERE userid = p_userid;
ECHO SELECT * FROM user_usage_table WHERE userid = p_userid ORDER BY sessionid;
echo SELECT l_status_byte, l_status_string from dual;
END//


CREATE OR REPLACE PROCEDURE AddCredit(p_userid bigint, p_extra_credit bigint, p_txnId TEXT)
AS
DECLARE
--
  l_status_byte int := 0;
  l_status_string text := '';
--
  q_user QUERY(userid BIGINT) =
     SELECT userid FROM user_table WHERE userid = p_userid;
--
  q_txn QUERY(user_txn_id BIGINT) =
     SELECT user_txn_id FROM user_recent_transactions WHERE userid = p_userid AND user_txn_id = p_txnId;
--
  l_found_userid bigint := null;
  l_found_txn_id bigint := null;
--
BEGIN
--
  FOR x IN COLLECT(q_user) LOOP
--
    l_found_userid := x.userid;
--
  END LOOP;
--
--
  FOR y IN COLLECT(q_txn) LOOP
--
    l_found_txn_id := y.user_txn_id;
--
  END LOOP;
--
  IF l_found_txn_id = p_txnId THEN
--
-- Txn has already happened
--
    l_status_byte = 46;
    l_status_string = 'Txn ' || p_txnId || ' already happened';
--
  ELSE
--
    IF l_found_userid = p_userid THEN
--
-- Update and report with as STATUS_CREDIT_ADDED
--
      UPDATE user_table 
      SET user_balance = user_balance + p_extra_credit
      WHERE userid = p_userid;
--
      INSERT INTO user_recent_transactions  
        (userid, user_txn_id, txn_time, approved_amount,spent_amount,purpose) 
      VALUES 
        (p_userid,p_txnId,NOW(),0,p_extra_credit,'Add Credit');       
--
--    Delete old TX records
--
      DELETE FROM user_recent_transactions 
      WHERE userid = p_userid
      AND txn_time < DATE_ADD(NOW(), INTERVAL -1 SECOND);
--
      l_status_byte = 56;
      l_status_string = p_extra_credit || ' added by Txn ' || p_txnId;
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
  END IF;
--
ECHO SELECT * FROM user_table WHERE userid = p_userid;
ECHO SELECT * FROM user_usage_table WHERE userid = p_userid ORDER BY sessionid;
echo SELECT l_status_byte, l_status_string from dual;
END//

DELIMITER ;

