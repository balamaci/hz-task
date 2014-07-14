package com.fortsoft.hztask.master;

import com.fortsoft.hztask.cluster.IClusterMasterService;
import com.fortsoft.hztask.common.task.Task;
import com.fortsoft.hztask.common.task.TaskKey;
import com.fortsoft.hztask.master.processor.FinishedTaskProcessor;
import com.fortsoft.hztask.master.processor.FinishedTaskProcessorFactory;
import com.hazelcast.core.IMap;

import java.io.Serializable;

/**
 * @author Serban Balamaci
 */
public class ClusterMasterServiceImpl implements IClusterMasterService {

    private MasterConfig masterConfig;

    private IMap<TaskKey, Task> tasks;

    public ClusterMasterServiceImpl(MasterConfig masterConfig) {
        this.masterConfig = masterConfig;
    }

    @Override
    public void handleFinishedTask(TaskKey taskKey, Serializable response) {
        Task task = tasks.remove(taskKey);
        FinishedTaskProcessorFactory finishedTaskProcessorFactory = masterConfig.
                getFinishedTaskProcessors().get(task.getClass());

        if(finishedTaskProcessorFactory != null) {
            FinishedTaskProcessor finishedTaskProcessor = finishedTaskProcessorFactory.getObject();
            finishedTaskProcessor.processSuccessful(task, response);
        }
    }

    @Override
    public void handleFailedTask(TaskKey taskKey, Throwable exception) {
        Task task = tasks.remove(taskKey);
        FinishedTaskProcessorFactory finishedTaskProcessorFactory = masterConfig.
                getFinishedTaskProcessors().get(task.getClass());

        if(finishedTaskProcessorFactory != null) {
            FinishedTaskProcessor finishedTaskProcessor = finishedTaskProcessorFactory.getObject();
            finishedTaskProcessor.processFailed(task, exception);
        }
    }

    public void setTasks(IMap<TaskKey, Task> tasks) {
        this.tasks = tasks;
    }
}
