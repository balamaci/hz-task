package ro.fortsoft.hztask.common.task;

import com.google.common.base.Objects;

import java.io.Serializable;
import java.util.UUID;

/**
 * @author sbalamaci
 */
public abstract class Task<I, O> implements Serializable {

    private final String id;

    private String clusterInstanceUuid;

    private int maxNoRetries;

    //higher is bigger priority
    private int priority = 0;

    private long creationDate;

    private int retryCounter = 0;

    public Task() {
        id = UUID.randomUUID().toString();
        creationDate = System.currentTimeMillis();
    }

    public String getTaskName() {
        return getClass().getName();
    }

    /**
     * Used in the routing strategy to determine the failure ratio of tasks with certain types and avoid
     * assigning tasks of the same type to members that have high failure ratios
     * for that type.
     * @return
     */
    public String getTaskType() {
        return getClass().getName();
    }

    public String getId() {
        return id;
    }

    public String getClusterInstanceUuid() {
        return clusterInstanceUuid;
    }

    public void setClusterInstanceUuid(String clusterInstanceUuid) {
        this.clusterInstanceUuid = clusterInstanceUuid;
    }

    public int getMaxNoRetries() {
        return maxNoRetries;
    }

    public void setMaxNoRetries(int maxNoRetries) {
        this.maxNoRetries = maxNoRetries;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public long getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(long creationDate) {
        this.creationDate = creationDate;
    }

    public int getRetryCounter() {
        return retryCounter;
    }

    public void incrementRetryCounter() {
        this.retryCounter++;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("id", id)
                .add("retryCounter", retryCounter)
                .add("priority", priority)
                .add("creationDate", creationDate)
                .toString();
    }
}
