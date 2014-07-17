package ro.fortsoft.hztask.agent.event.task;

import ro.fortsoft.hztask.common.task.TaskKey;

/**
 * @author Serban Balamaci
 */
public class TaskFailedEvent {

    private final TaskKey taskKey;

    private final Throwable exception;

    public TaskFailedEvent(TaskKey taskKey, Throwable exception) {
        this.taskKey = taskKey;
        this.exception = exception;
    }

    public TaskKey getTaskKey() {
        return taskKey;
    }

    public Throwable getException() {
        return exception;
    }
}
