package ro.fortsoft.hztask.callback;

import ro.fortsoft.hztask.common.task.TaskKey;
import ro.fortsoft.hztask.op.master.AbstractMasterOp;

import java.io.Serializable;

/**
 * @author Serban Balamaci
 */
public class NotifyMasterTaskFinishedOp extends AbstractMasterOp {

    private final TaskKey id;
    private final Serializable response;
    private final String agentUuid;

    public NotifyMasterTaskFinishedOp(TaskKey id, Serializable response, String agentUuid) {
        this.id = id;
        this.response = response;
        this.agentUuid = agentUuid;
    }


    @Override
    public Void call() throws Exception {
        getClusterMasterService().handleFinishedTask(id, response, agentUuid);
        return null;
    }
}