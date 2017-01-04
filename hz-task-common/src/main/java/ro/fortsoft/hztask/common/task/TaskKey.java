package ro.fortsoft.hztask.common.task;

import com.google.common.base.Objects;

import java.io.Serializable;

/**
 * Key that references Task in the distributed Hazelcast Map of {@link Task}
 *
 * @author Serban Balamaci
 */
public class TaskKey implements Serializable {

    private final String taskId;

    /** A counter that increases for each enqued task to have an order*/
    private long taskNo;

    public TaskKey(String taskId) {
        this.taskId = taskId;
    }

    public String getTaskId() {
        return taskId;
    }

    public long getTaskNo() {
        return taskNo;
    }

    public void setTaskNo(long taskNo) {
        this.taskNo = taskNo;
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

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("taskId", taskId)
                .toString();
    }
}
