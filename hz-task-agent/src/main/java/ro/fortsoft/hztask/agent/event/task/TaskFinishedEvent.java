package ro.fortsoft.hztask.agent.event.task;

import ro.fortsoft.hztask.common.task.Task;
import ro.fortsoft.hztask.common.task.TaskKey;

import java.io.Serializable;

/**
 * @author Serban Balamaci
 */
public class TaskFinishedEvent {

    private final TaskKey taskKey;
    private final Task task;

    private final Serializable result;

    public TaskFinishedEvent(TaskKey taskKey, Task task, Serializable result) {
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

    public Serializable getResult() {
        return result;
    }
}
