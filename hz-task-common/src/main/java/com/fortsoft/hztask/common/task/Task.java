package com.fortsoft.hztask.common.task;

import java.io.Serializable;
import java.util.UUID;

/**
 * @author sbalamaci
 */
public abstract class Task implements Serializable {

    private final String id;

    private String clusterInstanceUuid;

    private int nrOfTries;

    private Long delay;

    public Task() {
        id = UUID.randomUUID().toString();
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

    public void setDelayAfterProcess(long delayMs) {
        this.delay = delayMs;
    }

    public void setDelayBeforeProcess(long delayMs) {
        this.delay = - delayMs;
    }

    public Long getDelay() {
        return delay;
    }
}
