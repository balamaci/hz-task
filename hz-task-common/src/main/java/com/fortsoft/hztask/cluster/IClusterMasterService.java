package com.fortsoft.hztask.cluster;

import com.fortsoft.hztask.common.task.TaskKey;

import java.io.Serializable;

/**
 * @author Serban Balamaci
 */
public interface IClusterMasterService {

    public void handleFinishedTask(TaskKey taskKey, Serializable response);

    public void handleFailedTask(TaskKey taskKey, Throwable exception);

}
