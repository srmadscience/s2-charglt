
USE charglt; 

DELIMITER //

CREATE OR REPLACE PROCEDURE SendToKafka(p_userid bigint, p_txnId TEXT) AS
--
-- Do nothing unless re-created by send_to_kafka.sql
--
BEGIN
--
  SELECT userid, user_txn_id, txn_time, sessionid, approved_amount, spent_amount, purpose
  FROM user_recent_transactions
  WHERE userid = p_userid AND user_txn_id = p_txnId
  INTO KAFKA 'host.docker.internal:9092/charglt'
  FIELDS TERMINATED BY '\t' ENCLOSED BY '' ESCAPED BY '\\'
  LINES TERMINATED BY '\n' STARTING BY '';
--
END//


DELIMITER ;




