package com.fortsoft.hztask.cluster;

import com.fortsoft.hztask.common.task.TaskKey;

import java.io.Serializable;

/**
 * Interface through which the agents by sending ClusterOps lookup the cluster in the Op
 * and communicate with the ClusterMaster
 *
 * @author Serban Balamaci
 */
public interface IClusterMasterService {

    /**
     * Method called by the agent to handle a successfully finished task
     * @param taskKey taskKey
     * @param response the response from the task
     */
    void handleFinishedTask(TaskKey taskKey, Serializable response);

    public void handleFailedTask(TaskKey taskKey, Throwable exception);

}
