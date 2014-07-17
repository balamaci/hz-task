package ro.fortsoft.hztask.common.task;

import java.io.Serializable;

/**
 * @author Serban Balamaci
 */
public class TaskKey implements Serializable {

    private final String taskId;

    public TaskKey(String taskId) {
        this.taskId = taskId;
    }

    public String getTaskId() {
        return taskId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TaskKey taskKey = (TaskKey) o;

        if (!taskId.equals(taskKey.taskId)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return taskId.hashCode();
    }
}
