package ro.fortsoft.hztask.op.agent;

import ro.fortsoft.hztask.cluster.IClusterAgentService;
import ro.fortsoft.hztask.common.HzKeysConstants;
import ro.fortsoft.hztask.op.AbstractClusterOp;

/**
 * @author Serban Balamaci
 */
public abstract class AbstractAgentOp<T> extends AbstractClusterOp<T> {

    public IClusterAgentService getClusterAgentService() {
        return  (IClusterAgentService) getHzInstance().getUserContext().
                get(HzKeysConstants.USER_CONTEXT_CLUSTER_AGENT_SERVICE);
    }

}
