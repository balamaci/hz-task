package ro.fortsoft.hztask.op.agent;

/**
 * Message that signals the agent to shutdown
 *
 * @author Serban Balamaci
 */
public class ShutdownAgentOp extends AbstractAgentOp {

    @Override
    public Object call() throws Exception {
        getClusterAgentService().shutdown();
        return null;
    }
}
