
USE charglt;

drop procedure GetUsersWithMultipleSessions;
drop procedure showTransactions;
drop PROCEDURE GetUser;
drop PROCEDURE GetAndLockUser;
drop PROCEDURE UpdateLockedUser;
drop PROCEDURE UpsertUser;
drop PROCEDURE DelUser;
drop PROCEDURE ReportQuotaUsage;
drop PROCEDURE AddCredit;
drop PROCEDURE SendToKafka;

drop table user_table;
drop table user_usage_table;
drop table user_recent_transactions;
drop view current_locks;
drop view allocated_credit ;
drop view users_sessions ;
drop view recent_activity_out;
drop view recent_activity_in;
drop view cluster_activity_by_users ;
drop view cluster_activity ;
drop view last_cluster_activity ;
drop view cluster_users ;
