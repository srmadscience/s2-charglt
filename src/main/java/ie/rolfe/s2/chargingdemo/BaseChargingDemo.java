/*
 * Copyright (C) 2025 Volt Active Data Inc.
 *
 * Use of this source code is governed by an MIT
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */
/*
 * Copyright (C) 2025 David Rolfe
 *
 * Use of this source code is governed by an MIT
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */
package ie.rolfe.s2.chargingdemo;

import com.google.gson.Gson;
import com.singlestore.jdbc.Connection;
import com.singlestore.jdbc.Statement;
import org.voltdb.voltutil.stats.SafeHistogramCache;

import javax.rmi.ssl.SslRMIClientSocketFactory;
import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

/**
 * This is an abstract class that contains the actual logic of the demo code.
 */
public abstract class BaseChargingDemo {

    public static final long GENERIC_QUERY_USER_ID = 42;
    public static final int HISTOGRAM_SIZE_MS = 1000000;

    public static final String REPORT_QUOTA_USAGE = "ReportQuotaUsage";
    public static final String KV_PUT = "KV_PUT";
    public static final String KV_GET = "KV_GET";
    private static final String ADD_CREDIT = "ADD_CREDIT";

    public static SafeHistogramCache shc = SafeHistogramCache.getInstance();

    public static final String UNABLE_TO_MEET_REQUESTED_TPS = "UNABLE_TO_MEET_REQUESTED_TPS";
    public static final String EXTRA_MS = "EXTRA_MS";
    public static final int BATCH_SIZE = 50;

    /**
     * Print a formatted message.
     *
     * @param message
     */
    public static void msg(String message) {

        SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date now = new Date();
        String strDate = sdfDate.format(now);
        System.out.println(strDate + ":" + message);

    }

    /**
     * Connect to VoltDB using a comma delimited hostname list.
     *
     * @param commaDelimitedHostnames
     * @return
     * @throws Exception
     */
    @SuppressWarnings("deprecation")
    protected static Connection connectS2(String commaDelimitedHostnames, String appUser, String appPassword) throws Exception {
        Connection connection = null;
        String password = System.getenv("S2_PASSWORD");
        if (password == null) {
            password = appPassword;
        }
        String host = System.getenv("S2_HOST");
        if (host == null) {
            host = commaDelimitedHostnames;
        }
        String user = System.getenv("S2_USER");
        if (user == null) {
            user = appUser;
        }

        final String connectString = "jdbc:singlestore://" + host + ":3306/charglt?user=" + user + "&password=" + password;

        try {
            msg("Logging into S2");

            connection = (Connection) DriverManager.getConnection(connectString);

        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("DB connection failed.." + e.getMessage(), e);
        }

        return connection;

    }

    /**
     * Convenience method to generate a JSON payload.
     *
     * @param length
     * @return
     */
    protected static String getExtraUserDataAsJsonString(int length, Gson gson, Random r) {

        ExtraUserData eud = new ExtraUserData();

        eud.loyaltySchemeName = "HelperCard";
        eud.loyaltySchemeNumber = getNewLoyaltyCardNumber(r);

        StringBuffer ourText = new StringBuffer();

        for (int i = 0; i < length / 2; i++) {
            ourText.append(Integer.toHexString(r.nextInt(256)));
        }

        eud.mysteriousHexPayload = ourText.toString();

        return gson.toJson(eud);
    }


    protected static void deleteAllUsers(int minUserId, int maxUserId, int tpMs,
                                         Connection mainConnection) throws InterruptedException {
        try {
            final long startMsUpsert = System.currentTimeMillis();

            long currentMs = System.currentTimeMillis();
            int tpThisMs = 0;
            Random r = new Random();

            CallableStatement cs = mainConnection.prepareCall("call DelUser(?)\n");

            for (int i = minUserId; i <= maxUserId; i++) {

                if (tpThisMs++ > tpMs) {

                    while (currentMs == System.currentTimeMillis()) {
                        Thread.sleep(0, 50000);
                    }

                    currentMs = System.currentTimeMillis();
                    tpThisMs = 0;
                }


                cs.setLong(1, i);
                cs.execute();

                if (i % 100000 == 1) {
                    msg("Upserted " + i + " users...");
                    mainConnection.commit();
                }

            }

            msg("All " + minUserId + " -> " + maxUserId + " entries in queue, waiting for it to drain...");
            mainConnection.commit();

            long entriesPerMS = (maxUserId - minUserId) / (System.currentTimeMillis() - startMsUpsert);
            msg("Deleted " + entriesPerMS + " users per ms...");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * Create userCount users at tpMs per second.
     *
     * @param userCount
     * @param tpMs
     * @param ourJson
     * @param initialCredit
     * @param mainConnection
     */
    protected static void upsertAllUsers(int userCount, int tpMs, String ourJson, int initialCredit,
                                         Connection mainConnection) throws InterruptedException {
        try {
            final long startMsUpsert = System.currentTimeMillis();

            long currentMs = System.currentTimeMillis();
            int tpThisMs = 0;
            Random r = new Random();

            CallableStatement cs = null;
            cs = mainConnection.prepareCall("call UpsertUser(?,?,?,?,?,?)\n");

            for (int i = 0; i < userCount; i++) {

                if (tpThisMs++ > tpMs) {

                    while (currentMs == System.currentTimeMillis()) {
                        Thread.sleep(0, 50000);
                    }

                    currentMs = System.currentTimeMillis();
                    tpThisMs = 0;
                }


                cs.setLong(1, i);
                cs.setLong(2, r.nextInt(initialCredit));
                cs.setString(3, ourJson);
                cs.setString(4, "Initial Creation");
                cs.setTimestamp(5, new Timestamp(System.currentTimeMillis()));
                cs.setString(6, startMsUpsert + "_" + i);
                cs.execute();



                if (i % 100000 == 1) {
                    msg("Upserted " + i + " users...");
                    mainConnection.commit();
                }

            }

            msg("All " + userCount + " entries in queue, waiting for it to drain...");
            mainConnection.commit();

            long entriesPerMS = userCount / (System.currentTimeMillis() - startMsUpsert);
            msg("Upserted " + entriesPerMS + " users per ms...");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Convenience method to query a user a general stats and log the results.
     *
     * @param mainConnection
     * @param queryUserId
     */
    protected static void queryUserAndStats(Connection mainConnection, long queryUserId) {

//		// Query user #queryUserId...
//		msg("Query user #" + queryUserId + "...");
//		ConnectionResponse userResponse = mainConnection.callProcedure("GetUser", queryUserId);
//
//		for (int i = 0; i < userResponse.getResults().length; i++) {
//			msg(System.lineSeparator() + userResponse.getResults()[i].toFormattedString());
//		}
//
//		msg("Show amount of credit currently reserved for products...");
//		ConnectionResponse allocResponse = mainConnection.callProcedure("ShowCurrentAllocations__promBL");
//
//		for (int i = 0; i < allocResponse.getResults().length; i++) {
//			msg(System.lineSeparator() + allocResponse.getResults()[i].toFormattedString());
//		}
    }

    /**
     *
     * Convenience method to query all users who have a specific loyalty card id
     *
     * @param mainConnection
     * @param cardId
     */
    protected static void queryLoyaltyCard(Connection mainConnection, long cardId) throws IOException {

        // Query user #queryUserId...
        msg("Query card #" + cardId + "...");
//		ConnectionResponse userResponse = mainConnection.callProcedure("FindByLoyaltyCard", cardId);
//
//		for (int i = 0; i < userResponse.getResults().length; i++) {
//			msg(System.lineSeparator() + userResponse.getResults()[i].toFormattedString());
//		}

    }

    /**
     *
     * Run a key value store benchmark for userCount users at tpMs transactions per
     * millisecond and with deltaProportion records sending the entire record.
     *
     * @param userCount
     * @param tpMs
     * @param durationSeconds
     * @param globalQueryFreqSeconds
     * @param jsonsize
     * @param mainConnection
     * @param deltaProportion
     * @param extraMs
     * @return true if >=90% of requested throughput was achieved.
     * @throws InterruptedException
     */
    protected static boolean runKVBenchmark(int userCount, int tpMs, int durationSeconds, int globalQueryFreqSeconds,
                                            int jsonsize, Connection mainConnection, int deltaProportion, int extraMs) throws InterruptedException {

        double tps = 0;

        try {
            long lastGlobalQueryMs = 0;

            UserKVState[] userState = new UserKVState[userCount];

            Random r = new Random();

            Gson gson = new Gson();

            for (int i = 0; i < userCount; i++) {
                userState[i] = new UserKVState(i, shc);
            }

            final long startMsRun = System.currentTimeMillis();
            long currentMs = System.currentTimeMillis();
            int tpThisMs = 0;

            final long endtimeMs = System.currentTimeMillis() + (durationSeconds * 1000L);

            // How many transactions we've done...
            int tranCount = 0;
            int inFlightCount = 0;
            int lockCount = 0;
            int contestedLockCount = 0;
            int fullUpdate = 0;
            int deltaUpdate = 0;

            CallableStatement getAndLock = mainConnection.prepareCall("call GetAndLockUser(?,?)\n");
            CallableStatement updateLockedUser = mainConnection.prepareCall("call UpdateLockedUser(?,?,?,?)\n");


            while (endtimeMs > System.currentTimeMillis()) {

                if (tpThisMs++ > tpMs) {

                    while (currentMs == System.currentTimeMillis()) {
                        Thread.sleep(0, 50000);

                    }

                    sleepExtraMSIfNeeded(extraMs);

                    currentMs = System.currentTimeMillis();
                    tpThisMs = 0;
                }


                final long startMs = System.currentTimeMillis();

                // Find session to do a transaction for...
                int oursession = r.nextInt(userCount);

                // See if session already has an active transaction and avoid
                // it if it does.

                if (userState[oursession].isTxInFlight()) {

                    inFlightCount++;

                } else if (userState[oursession].getUserStatus() == UserKVState.STATUS_LOCKED_BY_SOMEONE_ELSE) {

                    if (userState[oursession].getOtherLockTimeMs() + ReferenceData.LOCK_TIMEOUT_MS < System
                            .currentTimeMillis()) {

                        userState[oursession].startTran();
                        userState[oursession].setStatus(UserKVState.STATUS_TRYING_TO_LOCK);

                        getAndLock.setLong(1, oursession);
                        getAndLock.setLong(2, oursession);
                        getAndLock.execute();
                        shc.reportLatency(BaseChargingDemo.KV_GET, startMs, "KV Get time", 2000);

                        lockCount++;

                    } else {
                        contestedLockCount++;
                    }

                } else if (userState[oursession].getUserStatus() == UserKVState.STATUS_UNLOCKED) {

                    userState[oursession].startTran();
                    userState[oursession].setStatus(UserKVState.STATUS_TRYING_TO_LOCK);
                    getAndLock.setLong(1, oursession);
                    getAndLock.setLong(2, oursession);
                    getAndLock.execute();
                    userState[oursession].setStatus(UserKVState.STATUS_LOCKED);
                    shc.reportLatency(BaseChargingDemo.KV_GET, startMs, "KV Get time", 2000);
                    lockCount++;

                } else if (userState[oursession].getUserStatus() == UserKVState.STATUS_LOCKED) {

                    userState[oursession].startTran();
                    userState[oursession].setStatus(UserKVState.STATUS_UPDATING);

                    if (deltaProportion > r.nextInt(101)) {

                        // Instead of sending entire JSON object across wire ask app to update loyalty
                        // number. For
                        // large values stored as JSON this can have a dramatic effect on network
                        // bandwidth
                        updateLockedUser.setLong(1, oursession);
                        updateLockedUser.setLong(2, oursession);
                        updateLockedUser.setString(3, getExtraUserDataAsJsonString(jsonsize, gson, r));
                        updateLockedUser.setString(4, "");
                        updateLockedUser.execute();
                        shc.reportLatency(BaseChargingDemo.KV_PUT, startMs, "KV Put Time", 2000);
                        deltaUpdate++;
                    } else {

                        // Instead of sending entire JSON object across wire ask app to update loyalty
                        // number. For
                        // large values stored as JSON this can have a dramatic effect on network
                        // bandwidth
                        updateLockedUser.setLong(1, oursession);
                        updateLockedUser.setLong(2, oursession);
                        updateLockedUser.setString(3, getExtraUserDataAsJsonString(jsonsize, gson, r));
                        updateLockedUser.setString(4, "");
                        updateLockedUser.execute();
                        shc.reportLatency(BaseChargingDemo.KV_PUT, startMs, "KV Put Time", 2000);
                        fullUpdate++;
                    }

                    userState[oursession].setStatus(UserKVState.STATUS_UNLOCKED);

                }

                tranCount++;
                userState[oursession].endTran(); //TODO - Fix when we make async

                if (tranCount % 100000 == 1) {
                    msg("Transaction " + tranCount);
                }
            }

            // See if we need to do global queries...
            if (lastGlobalQueryMs + (globalQueryFreqSeconds * 1000L) < System.currentTimeMillis()) {
                lastGlobalQueryMs = System.currentTimeMillis();

                queryUserAndStats(mainConnection, GENERIC_QUERY_USER_ID);

            }

            msg(tranCount + " transactions done...");
            msg("All entries in queue, waiting for it to drain...");
            //mainConnection.drain();
            msg("Queue drained...");

            long transactionsPerMs = tranCount / (System.currentTimeMillis() - startMsRun);
            msg("processed " + transactionsPerMs + " entries per ms while doing transactions...");

            long lockFailCount = 0;
            for (int i = 0; i < userCount; i++) {
                lockFailCount += userState[i].getLockedBySomeoneElseCount();
            }

            msg(inFlightCount + " events where a tx was in flight were observed");
            msg(lockCount + " lock attempts");
            msg(contestedLockCount + " contested lock attempts");
            msg(lockFailCount + " lock attempt failures");
            msg(fullUpdate + " full updates");
            msg(deltaUpdate + " delta updates");

            tps = tranCount;
            tps = tps / (System.currentTimeMillis() - startMsRun);
            tps = tps * 1000;

            reportRunLatencyStats(tpMs, tps);


        } catch (SQLException e) {
            msg(e.getMessage());
        }
        // Declare victory if we got >= 90% of requested TPS...
        return tps / (tpMs * 1000) > .9;
    }

    /**
     * Used when we need to really slow down below 1 tx per ms..
     *
     * @param extraMs an arbitrary extra delay.
     */
    private static void sleepExtraMSIfNeeded(int extraMs) {
        if (extraMs > 0) {
            try {
                Thread.sleep(extraMs);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    /**
     * Convenience method to remove unneeded records storing old allotments of
     * credit.
     *
     * @param mainConnection
     * @throws IOException
     * @throws SQLException
     */
    protected static void clearUnfinishedTransactions(Connection mainConnection) throws IOException, SQLException {

        msg("Clearing unfinished transactions from prior runs...");

        Statement stmt = mainConnection.createStatement();

        stmt.execute("TRUNCATE TABLE user_usage_table;");
        stmt.close();

        msg("...done");

    }

    /**
     *
     * Convenience method to clear outstaning locks between runs
     *
     * @param mainConnection
     * @throws IOException
     */
    protected static void unlockAllRecords(Connection mainConnection) throws IOException, SQLException {

        msg("Clearing locked sessions from prior runs...");

        Statement stmt = mainConnection.createStatement();

        stmt.execute(
                "UPDATE user_table SET user_softlock_sessionid = null, user_softlock_expiry = null WHERE user_softlock_sessionid IS NOT NULL;");
        mainConnection.commit();
        msg("...done");

    }

    /**
     *
     * Run a transaction benchmark for userCount users at tpMs per ms.
     *
     * @param userCount              number of users
     * @param tpMs                   transactions per milliseconds
     * @param durationSeconds
     * @param globalQueryFreqSeconds how often we check on global stats and a single
     *                               user
     * @param mainConnection
     * @return true if within 90% of targeted TPS
     */
    protected static boolean runTransactionBenchmark(int userCount, int tpMs, int durationSeconds,
                                                     int globalQueryFreqSeconds, Connection mainConnection, int extraMs) throws InterruptedException {

        // Used to track changes and be unique when we are running multiple threads
        final long pid = getPid();

        Random r = new Random();

        UserTransactionState[] users = new UserTransactionState[userCount];

        msg("Creating Connection records for " + users.length + " users");
        for (int i = 0; i < users.length; i++) {
            // We don't know a users credit till we've spoken to the server, so
            // we make an optimistic assumption...
            users[i] = new UserTransactionState(i, Long.MAX_VALUE);
        }

        final long startMsRun = System.currentTimeMillis();
        long currentMs = System.currentTimeMillis();
        int tpThisMs = 0;

        final long endtimeMs = System.currentTimeMillis() + (durationSeconds * 1000L);

        // How many transactions we've done...
        long tranCount = 0;
        long inFlightCount = 0;
        long addCreditCount = 0;
        long reportUsageCount = 0;
        long lastGlobalQueryMs = System.currentTimeMillis();

        msg("starting...");

        CallableStatement addCredit = null;
        CallableStatement reportUsage = null;

        try {
            addCredit = mainConnection.prepareCall("call AddCredit(?,?,?)\n");
            reportUsage = mainConnection.prepareCall("call ReportQuotaUsage(?,?,?,?,?)\n");


            while (endtimeMs > System.currentTimeMillis()) {

                if (tpThisMs++ > tpMs) {

                    while (currentMs == System.currentTimeMillis()) {
                        Thread.sleep(0, 50000);

                    }

                    sleepExtraMSIfNeeded(extraMs);

                    currentMs = System.currentTimeMillis();
                    tpThisMs = 0;
                }

                int randomuser = r.nextInt(userCount);

                final long startMs  = System.currentTimeMillis();

                if (users[randomuser].isTxInFlight()) {
                    inFlightCount++;
                } else {

                    users[randomuser].startTran();

                    if (users[randomuser].spendableBalance < 1000) {

                        addCreditCount++;

                        final long extraCredit = r.nextInt(1000) + 1000;

                        addCredit.setLong(1, randomuser);
                        addCredit.setLong(2, extraCredit);
                        addCredit.setString(3, "AddCreditOnShortage_" + pid + "_" + addCreditCount + "_" + System.currentTimeMillis());
                        addCredit.execute();
                        shc.reportLatency(BaseChargingDemo.ADD_CREDIT, startMs, "ADD_CREDIT", 2000);

                    } else {

                        reportUsageCount++;

                        long unitsUsed = (int) (users[randomuser].currentlyReserved * 0.9);
                        long unitsWanted = r.nextInt(100);
                        reportUsage.setLong(1, randomuser);
                        reportUsage.setLong(2, unitsUsed);
                        reportUsage.setLong(3, unitsWanted);
                        reportUsage.setLong(4, randomuser);
                        reportUsage.setString(5, "ReportQuotaUsage_" + pid + "_" + reportUsageCount + "_" + System.currentTimeMillis());
                        reportUsage.execute();
                        shc.reportLatency(BaseChargingDemo.REPORT_QUOTA_USAGE, startMs, "REPORT_QUOTA_USAGE", 2000);
                    }
                }

                tranCount++;
                users[randomuser].endTran(); //TODO - Fix when we make async

                if (tranCount % 100000 == 1) {
                    msg("Transaction " + tranCount);
                }

                // See if we need to do global queries...
                if (lastGlobalQueryMs + (globalQueryFreqSeconds * 1000L) < System.currentTimeMillis()) {
                    lastGlobalQueryMs = System.currentTimeMillis();

                    queryUserAndStats(mainConnection, GENERIC_QUERY_USER_ID);

                }

            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        msg("finished adding transactions to queue");
        //mainConnection.drain();
        msg("Queue drained");

        long elapsedTimeMs = System.currentTimeMillis() - startMsRun;
        msg("Processed " + tranCount + " transactions in " + elapsedTimeMs + " milliseconds");

        double tps = tranCount;
        tps = tps / (elapsedTimeMs / 1000);

        msg("TPS = " + tps);

        msg("Add Credit calls = " + addCreditCount);
        msg("Report Usage calls = " + reportUsageCount);
        msg("Skipped because transaction was in flight = " + inFlightCount);

        reportRunLatencyStats(tpMs, tps);

        // Declare victory if we got >= 90% of requested TPS...
        return tps / (tpMs * 1000) > .9;
    }

    /**
     * Turn latency stats into a grepable string
     *
     * @param tpMs target transactions per millisecond
     * @param tps  observed TPS
     */
    private static void reportRunLatencyStats(int tpMs, double tps) {
        StringBuffer oneLineSummary = new StringBuffer("GREPABLE SUMMARY:");

        oneLineSummary.append(tpMs);
        oneLineSummary.append(':');

        oneLineSummary.append(tps);
        oneLineSummary.append(':');

        SafeHistogramCache.getProcPercentiles(shc, oneLineSummary, REPORT_QUOTA_USAGE);

        SafeHistogramCache.getProcPercentiles(shc, oneLineSummary, KV_PUT);

        SafeHistogramCache.getProcPercentiles(shc, oneLineSummary, KV_GET);

        msg(oneLineSummary.toString());

        msg(shc.toString());
    }

    /**
     * Get Linux process ID - used for pseudo unique ids
     *
     * @return Linux process ID
     */
    private static long getPid() {
        return ProcessHandle.current().pid();
    }

    /**
     * Return a loyalty card number
     *
     * @param r
     * @return a random loyalty card number between 0 and 1 million
     */
    private static long getNewLoyaltyCardNumber(Random r) {
        return System.currentTimeMillis() % 1000000;
    }

    /**
     * get EXTRA_MS env variable if set
     *
     * @return extraMs
     */
    public static int getExtraMsIfSet() {

        int extraMs = 0;

        String extraMsEnv = System.getenv(EXTRA_MS);

        if (extraMsEnv != null && extraMsEnv.length() > 0) {
            msg("EXTRA_MS is '" + extraMsEnv + "'");
            extraMs = Integer.parseInt(extraMsEnv);
        }

        return extraMs;
    }

}
