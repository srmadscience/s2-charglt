/*
 * Copyright (C) 2025 David Rolfe
 *
 * Use of this source code is governed by an MIT
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */

package ie.rolfe.s2.chargingdemo;

import com.singlestore.jdbc.Connection;
import com.singlestore.jdbc.client.result.ResultSetMetaData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.*;

import static ie.rolfe.s2.chargingdemo.ReferenceData.STATUS_RECORD_HAS_BEEN_SOFTLOCKED;
import static ie.rolfe.s2.chargingdemo.ReferenceData.STATUS_USER_DOESNT_EXIST;
import static org.junit.jupiter.api.Assertions.fail;

class DDLTest {


    final long TEST_USER_ID = 42;
    final long BAD_USER_ID = 2;
    final long TEST_SESSION_ID = 12;
    final long TEST_BALANCE = 1000;
    final long TEST_ALLOCATED = 10;
    final String[] setupDML = {"INSERT INTO user_table  " +
            "(userid ,user_json_object ,user_last_seen ,user_softlock_sessionid ,user_softlock_expiry, user_balance)" +
            " VALUES " +
            "(" + TEST_USER_ID + ", NULL, NOW(), NULL, NULL, " + TEST_BALANCE + ");"
            , "INSERT INTO user_recent_transactions " +
            "(userid,user_txn_id,txn_time ,sessionid,approved_amount ,spent_amount ,purpose)" +
            " VALUES " +
            "(" + TEST_USER_ID + ",'Create1',NOW()," + TEST_SESSION_ID + ", 0, " + TEST_ALLOCATED + ", 'Setup');"
            , "INSERT INTO user_usage_table (userid,allocated_amount ,sessionid ,lastdate )" +
            " VALUES " +
            "(" + TEST_USER_ID + "," + TEST_ALLOCATED + "," + TEST_SESSION_ID + ", NOW());"};
    final String[] tearDownTables = {"delete from user_table", "delete from user_recent_transactions", "delete from user_usage_table"};
    Connection connection = null;

    @BeforeEach
    void setUp() {
        BaseChargingDemo.msg("setup");
        String password = System.getenv("S2_PASSWORD");
        if (password == null) {
            password = "nevadaeagle";
        }
        String host = System.getenv("S2_HOST");
        if (host == null) {
            host = "10.13.1.101";
        }
        String user = System.getenv("S2_USER");
        if (user == null) {
            user = "root";
        }

        final String connectString = "jdbc:singlestore://" + host + ":3306/charglt?user=" + user + "&password=" + password;
        try {
            BaseChargingDemo.msg("Connecting to " + connectString);
            connection = (Connection) DriverManager.getConnection(connectString);

            for (int i = 0; i < tearDownTables.length; i++) {

                BaseChargingDemo.msg(tearDownTables[i]);
                PreparedStatement ps = connection.prepareStatement(tearDownTables[i]);
                ps.executeUpdate();
            }

            for (int i = 0; i < setupDML.length; i++) {

                BaseChargingDemo.msg(setupDML[i]);
                PreparedStatement ps = connection.prepareStatement(setupDML[i]);
                ps.executeUpdate();
            }

            connection.commit();

        } catch (SQLException e) {
            BaseChargingDemo.msg(e.getMessage());
            fail(e);
        }
    }

    @AfterEach
    void tearDown() {
        BaseChargingDemo.msg("teardown");
        try {
            if (connection != null) {

                for (int i = 0; i < tearDownTables.length; i++) {

                    BaseChargingDemo.msg(tearDownTables[i]);
                    PreparedStatement ps = connection.prepareStatement(tearDownTables[i] + ";");
                    ps.executeUpdate();
                }


                connection.commit();
                connection.close();
            }
        } catch (SQLException e) {
            BaseChargingDemo.msg(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @Test
    void Smoketest() {
        BaseChargingDemo.msg("smoketest");
        //setUp();
        try {
            PreparedStatement ps = connection.prepareStatement("SELECT * FROM user_table;");
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                long userid = rs.getLong("USERID");
                BaseChargingDemo.msg(this.getClass().getName() + ":Found " + userid);
            }
        } catch (SQLException e) {
            BaseChargingDemo.msg(e.getMessage());
            fail(e);
            throw new RuntimeException(e);
        }

        //tearDown();
    }

    @Test
    void GetUsersWithMultipleSessions() {
        BaseChargingDemo.msg("getUsersWithMultipleSessions");
        try {
            CallableStatement cs = connection.prepareCall("call GetUsersWithMultipleSessions()\n");
            cs.execute();
            ResultSet rs = cs.getResultSet();

            int rowCount = 0;
            while (rs.next()) {
                rowCount++;
                long userid = rs.getLong("USERID");
                BaseChargingDemo.msg(this.getClass().getName() + ":Found " + userid);
            }

            if (rowCount > 0) {
                fail("getUsersWithMultipleSessions: rows found");
            }

            PreparedStatement psExtraSession = connection.prepareStatement("INSERT INTO user_usage_table (userid,allocated_amount ,sessionid ,lastdate )" +
                    " VALUES " +
                    "(" + TEST_USER_ID + "," + TEST_ALLOCATED + "," + TEST_SESSION_ID + 1 + ", NOW());");
            psExtraSession.executeUpdate();

            cs.execute();
            rs = cs.getResultSet();

            rowCount = 0;
            while (rs.next()) {
                rowCount++;
                long userid = rs.getLong("USERID");
                BaseChargingDemo.msg(this.getClass().getName() + ":Found " + userid);
            }

            if (rowCount != 1) {
                fail("getUsersWithMultipleSessions: mo rows found");
            }


        } catch (SQLException e) {
            BaseChargingDemo.msg(e.getMessage());
            fail(e);
            throw new RuntimeException(e);
        }

    }

    @Test
    void ShowTransactions() {
        BaseChargingDemo.msg("ShowTransactions");
        try {
            CallableStatement cs = connection.prepareCall("call showTransactions(?)\n");
            cs.setLong(1, TEST_USER_ID);
            cs.execute();
            ResultSet rs = cs.getResultSet();

            int rowCount = 0;
            while (rs.next()) {
                rowCount++;
                long userid = rs.getLong("USERID");
                BaseChargingDemo.msg(this.getClass().getName() + ":Found " + userid);
            }


            if (rowCount != 1) {
                fail("getUsersWithMultipleSessions: 1 row should have been found");
            }


        } catch (SQLException e) {
            BaseChargingDemo.msg(e.getMessage());
            fail(e);
            throw new RuntimeException(e);
        }

    }

    @Test
    void ShowTransactionsBadUser() {
        BaseChargingDemo.msg("ShowTransactions");
        try {
            CallableStatement cs = connection.prepareCall("call showTransactions(?)\n");
            cs.setLong(1, TEST_USER_ID + 1);
            cs.execute();
            ResultSet rs = cs.getResultSet();

            int rowCount = 0;
            while (rs.next()) {
                rowCount++;
                long userid = rs.getLong("USERID");
                BaseChargingDemo.msg(this.getClass().getName() + ":Found " + userid);
            }


            if (rowCount != 0) {
                fail("getUsersWithMultipleSessions: 0 row should have been found");
            }


        } catch (SQLException e) {
            BaseChargingDemo.msg(e.getMessage());
            fail(e);
            throw new RuntimeException(e);
        }

    }

    @Test
    void GetUser() {
        BaseChargingDemo.msg("GetUser");
        try {
            CallableStatement cs = connection.prepareCall("call GetUser(?)\n");
            cs.setLong(1, TEST_USER_ID);
            cs.execute();

            int rowCount = 0;

            do {
                try (ResultSet resultSet = cs.getResultSet()) {
                    if (resultSet != null) {
                        while (resultSet.next()) {
                            rowCount++;
                            long userid = resultSet.getLong("userid");
                            BaseChargingDemo.msg(this.getClass().getName() + ":Found " + userid);
                        }
                    }
                } catch (SQLException e) {
                    BaseChargingDemo.msg(e.getMessage());
                    fail(e);
                    throw new RuntimeException(e);
                }
            } while (cs.getMoreResults());

            if (rowCount != 3) {
                fail("getUsersWithMultipleSessions: 3 rows should have been found");
            }


        } catch (SQLException e) {
            BaseChargingDemo.msg(e.getMessage());
            fail(e);
            throw new RuntimeException(e);
        }

    }

    @Test
    void GetAndLockUserNoUser() {
        BaseChargingDemo.msg("GetAndLockUserNoUser");
        try {
            CallableStatement cs = connection.prepareCall("call GetAndLockUser(?,?)\n");
            cs.setLong(1, BAD_USER_ID);
            cs.setLong(2, TEST_SESSION_ID);
            cs.execute();

            int rowCount = 0;
            long l_status_byte = Long.MIN_VALUE;

            do {
                try (ResultSet resultSet = cs.getResultSet()) {
                    if (resultSet != null && hasColumn(resultSet, "l_status_byte")) {

                        while (resultSet.next()) {
                            rowCount++;
                            l_status_byte = resultSet.getLong("l_status_byte");
                            BaseChargingDemo.msg(this.getClass().getName() + ":Found " + l_status_byte);
                        }
                    }
                } catch (SQLException e) {
                    BaseChargingDemo.msg(e.getMessage());
                    fail(e);
                    throw new RuntimeException(e);
                }
            } while (cs.getMoreResults());

            if (l_status_byte != STATUS_USER_DOESNT_EXIST) {
                fail("GetAndLockUserNoUser: return code should be " + STATUS_USER_DOESNT_EXIST + ". " + " got " + l_status_byte);
            }


        } catch (SQLException e) {
            BaseChargingDemo.msg(e.getMessage());
            fail(e);
            throw new RuntimeException(e);
        }

    }

    @Test
    void GetAndLockUserRealUser() {
        BaseChargingDemo.msg("GetAndLockUserRealUser");
        try {

            //
            // make sure is unlocked...
            long lockingSessionId = getLongFromUserTable("user_softlock_sessionid");

            if (lockingSessionId != Long.MIN_VALUE) {
                fail("GetAndLockUserRealUser: Saw " + lockingSessionId + ", expected null ");
            }


            CallableStatement cs = connection.prepareCall("call GetAndLockUser(?,?)\n");
            cs.setLong(1, TEST_USER_ID);
            cs.setLong(2, TEST_SESSION_ID);
            cs.execute();

            int rowCount = 0;
            long l_status_byte = Long.MIN_VALUE;

            do {
                try (ResultSet resultSet = cs.getResultSet()) {
                    if (resultSet != null && hasColumn(resultSet, "l_status_byte")) {

                        while (resultSet.next()) {
                            rowCount++;
                            l_status_byte = resultSet.getLong("l_status_byte");
                            BaseChargingDemo.msg(this.getClass().getName() + ":Found " + l_status_byte);
                        }
                    }
                } catch (SQLException e) {
                    BaseChargingDemo.msg(e.getMessage());
                    fail(e);
                    throw new RuntimeException(e);
                }
            } while (cs.getMoreResults());

            if (l_status_byte != STATUS_RECORD_HAS_BEEN_SOFTLOCKED) {
                fail("GetAndLockUserRealUser: return code should be " + STATUS_RECORD_HAS_BEEN_SOFTLOCKED + ". " + " got " + l_status_byte);
            }


            //
            // See if our session ID is now there...
            lockingSessionId = getLongFromUserTable("user_softlock_sessionid");

            if (lockingSessionId != TEST_SESSION_ID) {
                fail("GetAndLockUserRealUser: Saw " + lockingSessionId + ", expected " + TEST_SESSION_ID);
            }

        } catch (SQLException e) {
            BaseChargingDemo.msg(e.getMessage());
            fail(e);
            throw new RuntimeException(e);
        }

    }

    @Test
    void DelUser() {
        BaseChargingDemo.msg("DelUser");
        try {

            long userId = getLongFromUserTable("userid");

            if (userId == Long.MIN_VALUE) {
                fail("DelUser: User not found at start");
            }


            CallableStatement cs = connection.prepareCall("call DelUser(?)\n");
            cs.setLong(1, TEST_USER_ID);
            cs.execute();

        } catch (SQLException e) {
            BaseChargingDemo.msg(e.getMessage());
            fail(e);
            throw new RuntimeException(e);
        }


        long userId = getLongFromUserTable("userid");

        if (userId != Long.MIN_VALUE) {
            fail("DelUser: User still found at end");
        }


    }

    private boolean hasColumn(ResultSet resultSet, String columnName) throws SQLException {

        ResultSetMetaData rsmd = (ResultSetMetaData) resultSet.getMetaData();

        for (int i = 1; i <= rsmd.getColumnCount(); i++) {
            if (rsmd.getColumnName(i).equals(columnName)) {
                return true;
            }
        }
        return false;
    }


    private long getLongFromUserTable(String fieldName) {

        long value = Long.MIN_VALUE;

        try {
            CallableStatement cs = connection.prepareCall("call GetUser(?)\n");
            cs.setLong(1, TEST_USER_ID);
            cs.execute();

            int rowCount = 0;


            do {
                try (ResultSet resultSet = cs.getResultSet()) {
                    if (resultSet != null && hasColumn(resultSet, fieldName)) {
                        while (resultSet.next()) {
                            rowCount++;
                            value = resultSet.getLong(fieldName);
                            if (resultSet.wasNull()) {
                                value = Long.MIN_VALUE;
                            }
                        }
                    }
                } catch (SQLException e) {
                    BaseChargingDemo.msg(e.getMessage());
                    fail(e);
                    throw new RuntimeException(e);
                }
            } while (cs.getMoreResults());

        } catch (SQLException e) {
            BaseChargingDemo.msg(e.getMessage());
            fail(e);
            throw new RuntimeException(e);
        }

        return value;
    }
}