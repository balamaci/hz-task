package ro.fortsoft.hztask.op.agent;

/**
 * Message that signals the agent to output debug statistics
 *
 * @author Serban Balamaci
 */
public class OutputDebugStatsOp extends AbstractAgentOp {

    @Override
    public Object call() throws Exception {
        getClusterAgentService().outputDebugStatistics();
        return null;
    }
}
