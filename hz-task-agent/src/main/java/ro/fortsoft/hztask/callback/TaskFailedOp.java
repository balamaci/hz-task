package ro.fortsoft.hztask.callback;

import ro.fortsoft.hztask.common.task.TaskKey;
import ro.fortsoft.hztask.op.master.AbstractMasterOp;

/**
 * @author Serban Balamaci
 */
public class TaskFailedOp extends AbstractMasterOp {

    private final TaskKey id;

    private final Throwable exception;

    public TaskFailedOp(TaskKey id, Throwable exception) {
        this.id = id;
        this.exception = exception;
    }

    @Override
    public Object call() throws Exception {
        getClusterMasterService().handleFailedTask(id, exception);
        return null;
    }
}
