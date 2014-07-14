package com.fortsoft.hztask.master.processor;

import com.fortsoft.hztask.common.task.Task;
import com.fortsoft.hztask.master.ClusterMasterServiceImpl;

/**
 * @author Serban Balamaci
 */
public abstract class FinishedTaskProcessor {

    public abstract void processSuccessful(Task task, Object taskResult);

    public abstract void processFailed(Task task, Throwable throwable);

    private ClusterMasterServiceImpl clusterMasterService;


}
