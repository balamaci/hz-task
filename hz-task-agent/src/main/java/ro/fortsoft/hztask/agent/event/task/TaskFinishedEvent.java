package ro.fortsoft.hztask.agent.event.task;

import ro.fortsoft.hztask.common.task.Task;
import ro.fortsoft.hztask.common.task.TaskKey;

import java.io.Serializable;

/**
 * @author Serban Balamaci
 */
public class TaskFinishedEvent<T extends Serializable> {

    private final TaskKey taskKey;
    private final Task task;

    private final T result;

    /**
     * Constr.
     * @param taskKey taskKey
     * @param task Task
     * @param result after the task was processed
     */
    public TaskFinishedEvent(TaskKey taskKey, Task task, T result) {
        this.taskKey = taskKey;
        this.task = task;
        this.result = result;
    }

    public TaskKey getTaskKey() {
        return taskKey;
    }

    public Task getTask() {
        return task;
    }

    public T getResult() {
        return result;
    }
}
