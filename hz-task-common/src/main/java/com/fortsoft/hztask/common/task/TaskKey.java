package com.fortsoft.hztask.common.task;

import com.hazelcast.core.PartitionAware;

import java.io.Serializable;

/**
 * @author Serban Balamaci
 */
public class TaskKey implements Serializable, PartitionAware<String> {

    private final String clusterInstanceUuid;

    private final String taskId;

    public TaskKey(String clusterInstanceUuid, String taskId, String taskClassName) {
        this.clusterInstanceUuid = clusterInstanceUuid;
        this.taskId = taskId;
    }

    @Override
    public String getPartitionKey() {
        return clusterInstanceUuid;
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
