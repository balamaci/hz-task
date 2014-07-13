package com.fortsoft.hztask.op.agent;

import com.fortsoft.hztask.cluster.IClusterAgentService;
import com.fortsoft.hztask.op.AbstractClusterOp;

/**
 * @author Serban Balamaci
 */
public abstract class AbstractAgentOp<T> extends AbstractClusterOp<T> {

    public IClusterAgentService getClusterAgentService() {
        return  (IClusterAgentService) getHzInstance().getConfig().
                getUserContext().get("clusterAgentService");
    }

}
