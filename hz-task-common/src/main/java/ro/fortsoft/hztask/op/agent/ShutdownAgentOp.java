package ro.fortsoft.hztask.op.agent;

/**
 * Message that signals the agent to shutdown
 *
 * @author Serban Balamaci
 */
public class ShutdownAgentOp extends AbstractAgentOp<Void> {

    @Override
    public Void call() throws Exception {
        getClusterAgentService().shutdown();
        return null;
    }
}
