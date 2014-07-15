package ro.fortsoft.hztask.op.master;

import ro.fortsoft.hztask.cluster.IClusterMasterService;
import ro.fortsoft.hztask.op.AbstractClusterOp;

/**
 * @author Serban Balamaci
 */
public abstract class AbstractMasterOp extends AbstractClusterOp {

    public IClusterMasterService getClusterMasterService() {
        return  (IClusterMasterService) getHzInstance().getConfig().
                getUserContext().get("clusterMasterService");
    }
}
