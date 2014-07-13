package com.fortsoft.hztask.op.agent;

import com.fortsoft.hztask.cluster.IClusterAgentService;

/**
 * @author Serban Balamaci
 */
public class AskAgentReadyOp extends AbstractAgentOp<Boolean> {

    @Override
    public Boolean call() throws Exception {
        IClusterAgentService clusterAgentService = getClusterAgentService();
        return clusterAgentService != null;
    }
}
