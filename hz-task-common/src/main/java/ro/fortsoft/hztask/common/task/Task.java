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

    private int nrOfTries;

    //higher is bigger priority
    private int priority = 0;

    private long creationDate;

    private long internalCounter;

    public Task() {
        id = UUID.randomUUID().toString();
        creationDate = System.currentTimeMillis();
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

    public int getNrOfTries() {
        return nrOfTries;
    }

    public void setNrOfTries(int nrOfTries) {
        this.nrOfTries = nrOfTries;
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

    public long getInternalCounter() {
        return internalCounter;
    }

    public void setInternalCounter(long internalCounter) {
        this.internalCounter = internalCounter;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("id", id)
                .add("nrOfTries", nrOfTries)
                .add("priority", priority)
                .add("creationDate", creationDate)
                .toString();
    }
}
