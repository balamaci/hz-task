package ro.fortsoft.hztask.op.agent;

import ro.fortsoft.hztask.cluster.IClusterAgentService;
import ro.fortsoft.hztask.op.AbstractClusterOp;

/**
 * @author Serban Balamaci
 */
public abstract class AbstractAgentOp<T> extends AbstractClusterOp<T> {

    public IClusterAgentService getClusterAgentService() {
        return  (IClusterAgentService) getHzInstance().getConfig().
                getUserContext().get("clusterAgentService");
    }

}
