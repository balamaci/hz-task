package ro.fortsoft.hztask.callback;

import ro.fortsoft.hztask.common.task.TaskKey;
import ro.fortsoft.hztask.op.master.AbstractMasterOp;

import java.io.Serializable;

/**
 * @author Serban Balamaci
 */
public class TaskFinishedOp extends AbstractMasterOp {

    private final TaskKey id;

    private final Serializable response;

    public TaskFinishedOp(TaskKey id, Serializable response) {
        this.id = id;
        this.response = response;
    }


    @Override
    public Void call() throws Exception {
        getClusterMasterService().handleFinishedTask(id, response);
        return null;
    }
}