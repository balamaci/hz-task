package ro.fortsoft.hztask.common.task;

import java.util.concurrent.Callable;

/**
 * @author sbalamaci
 */
public class TaskResponse<T> implements Callable<T> {

    private final String id;

    private final Exception taskFailedCause;

    public TaskResponse(String id, Exception taskFailedCause) {
        this.id = id;
        this.taskFailedCause = taskFailedCause;
    }

    public TaskResponse(String id) {
        this.id = id;
        taskFailedCause = null;
    }

    public String getId() {
        return id;
    }

    @Override
    public T call() throws Exception {
        return null;
    }
}
