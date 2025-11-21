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

package ie.rolfe.s2.chargingdemo.callbacks;


import ie.rolfe.s2.*;

/**
 * Simple callback that complains if something went badly wrong.
 * 
 * @author drolfe
 *
 */
public class ComplainOnErrorCallback implements ProcedureCallback {

    @Override
    public void clientCallback(ClientResponse arg0) throws Exception {

        if (arg0.getStatus() != ClientResponse.SUCCESS) {
            BaseChargingDemo.msg("Error Code " + arg0.getStatusString());
        }

    }

}
