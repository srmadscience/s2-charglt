package ie.rolfe.s2.chargingdemo;

import com.singlestore.jdbc.Connection;

import java.sql.CallableStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Random;

public class ChargingDemoTransactionWorker extends BaseChargingDemo implements Runnable {

    public static final int MAX_QUEUE_SIZE = 5;
    private static final int SLEEP_NANOS = 500000;
    final ArrayList<Request> requestQueue = new ArrayList<Request>();
    int workerId;
    Connection mainConnection;
    Random r;
    CallableStatement addCredit;
    CallableStatement reportUsage;
    UserTransactionState[] users;
    boolean keepGoing = true;


    public ChargingDemoTransactionWorker(int workerId, String hostlist, String username, String password, Random r, UserTransactionState[] users) {
        this.workerId = workerId;
        this.r = r;
        this.users = users;
        try {
            mainConnection = connectS2(hostlist, username, password);
            addCredit = mainConnection.prepareCall(CALL_ADD_CREDIT);
            reportUsage = mainConnection.prepareCall(CALL_REPORT_QUOTA_USAGE);
        } catch (Exception e) {
            throw new RuntimeException(e);

        }
    }

    public void setKeepGoing(boolean keepGoing) {
        this.keepGoing = keepGoing;
        workerMsg("KeepGoing set to " + keepGoing + ". Queue size: " + requestQueue.size());
    }

    public boolean addRequest(Request request) {
        synchronized (requestQueue) {

            if (requestQueue.size() >= MAX_QUEUE_SIZE) {
                shc.incCounter("QUEUE_OVERFLOW");
                return false;
            }

            requestQueue.add(request);
            return true;
        }
    }

    @Override
    public void run() {

        while (keepGoing) {

            while (!requestQueue.isEmpty()) {
                try {
                    Request request = null;
                    synchronized (requestQueue) {
                        request = requestQueue.removeFirst();
                    }
                    final long startMs = System.currentTimeMillis();
                    doTransaction(users, request.randomuser, r, addCredit
                            , request.pid, startMs, reportUsage, request.txId);

                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }

            try {
                Thread.sleep(0, SLEEP_NANOS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

        }

        try {
            mainConnection.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    private void workerMsg(String message) {

        msg(workerId + ": " + message);

    }
}
