package com.fortsoft.hztask.op.master;

import com.fortsoft.hztask.cluster.IClusterMasterService;
import com.fortsoft.hztask.op.AbstractClusterOp;

/**
 * @author Serban Balamaci
 */
public abstract class AbstractMasterOp extends AbstractClusterOp {

    public IClusterMasterService getClusterMasterService() {
        return  (IClusterMasterService) getHzInstance().getConfig().
                getUserContext().get("clusterMasterService");
    }
}
