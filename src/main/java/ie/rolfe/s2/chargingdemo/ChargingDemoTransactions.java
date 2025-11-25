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


import com.singlestore.jdbc.Connection;
import com.singlestore.jdbc.client.Client;

import java.util.Arrays;


public class ChargingDemoTransactions extends BaseChargingDemo {

    /**
     * @param args
     */
    public static void main(String[] args) {

        msg("Parameters:" + Arrays.toString(args));

        if (args.length != 7) {
            msg("Usage: hostnames recordcount tpms durationseconds queryseconds  username password");
            System.exit(1);
        }

        // Comma delimited list of hosts...
        String hostlist = args[0];

        // How many users
        int userCount = Integer.parseInt(args[1]);

        // Target transactions per millisecond.
        int tpMs = Integer.parseInt(args[2]);

        // Runtime for TRANSACTIONS in seconds.
        int durationSeconds = Integer.parseInt(args[3]);

        // How often we do global queries...
        int globalQueryFreqSeconds = Integer.parseInt(args[4]);

        String username = args[5];
        String password = args[6];


        // Extra delay for testing really slow hardware
        int extraMs = getExtraMsIfSet();

        try {
            // A  Client object maintains multiple connections to all the
            // servers in the cluster.
            Connection mainConnection = connectS2(hostlist, username,password);

            clearUnfinishedTransactions(mainConnection);

            boolean ok = runTransactionBenchmark(userCount, tpMs, durationSeconds, globalQueryFreqSeconds, mainConnection, extraMs);

            msg("Closing connection...");
            mainConnection.close();

            if (ok) {
                System.exit(0);
            }

            msg(UNABLE_TO_MEET_REQUESTED_TPS);
            System.exit(1);

        } catch (Exception e) {
            msg(e.getMessage());
        }

    }

}
