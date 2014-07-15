package ro.fortsoft.hztask.op.agent;

import ro.fortsoft.hztask.cluster.IClusterAgentService;

/**
 * @author Serban Balamaci
 */
public class AskAgentReadyOp extends AbstractAgentOp<Boolean> {

    @Override
    public Boolean call() throws Exception {
        IClusterAgentService clusterAgentService = getClusterAgentService();
        System.out.println("Cluster AgentService " + clusterAgentService);
        return clusterAgentService != null;
    }

}
