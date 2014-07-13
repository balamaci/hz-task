package com.fortsoft.hztask.common.task;

import java.io.Serializable;
import java.util.UUID;

/**
 * @author sbalamaci
 */
public abstract class Task implements Serializable {

    private final String id;

    private String clusterInstanceUuid;

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
}
