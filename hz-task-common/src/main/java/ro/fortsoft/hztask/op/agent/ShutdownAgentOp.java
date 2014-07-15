package ro.fortsoft.hztask.op.agent;

/**
 * @author Serban Balamaci
 */
public class ShutdownAgentOp extends AbstractAgentOp {

    @Override
    public Object call() throws Exception {
        getClusterAgentService().shutdown();
        return null;
    }
}
