package com.fortsoft.hztask.master;

import com.fortsoft.hztask.cluster.IClusterMasterService;
import com.fortsoft.hztask.common.task.Task;
import com.fortsoft.hztask.common.task.TaskKey;
import com.fortsoft.hztask.master.processor.FinishedTaskProcessor;
import com.fortsoft.hztask.master.processor.FinishedTaskProcessorFactory;
import com.hazelcast.core.IMap;

import java.io.Serializable;
import java.util.Map;

/**
 * @author Serban Balamaci
 */
public class ClusterMasterServiceImpl implements IClusterMasterService {

    private Map<Class, FinishedTaskProcessorFactory> finishedTaskProcessors;

    private IMap<TaskKey, Task> tasks;

    @Override
    public void handleFinishedTask(TaskKey taskKey, Serializable response) {
        Task task = tasks.remove(taskKey);
        FinishedTaskProcessorFactory finishedTaskProcessorFactory =
                finishedTaskProcessors.get(task.getClass());

        if(finishedTaskProcessorFactory != null) {
            FinishedTaskProcessor finishedTaskProcessor = finishedTaskProcessorFactory.getObject();
            finishedTaskProcessor.processSuccessful(task, response);
        }
    }

    @Override
    public void handleFailedTask(TaskKey taskKey, Throwable exception) {
        Task task = tasks.remove(taskKey);
        FinishedTaskProcessorFactory finishedTaskProcessorFactory =
                finishedTaskProcessors.get(task.getClass());

        if(finishedTaskProcessorFactory != null) {
            FinishedTaskProcessor finishedTaskProcessor = finishedTaskProcessorFactory.getObject();
            finishedTaskProcessor.processFailed(task, exception);
        }
    }

    public void setFinishedTaskProcessors(Map<Class, FinishedTaskProcessorFactory> finishedTaskProcessors) {
        this.finishedTaskProcessors = finishedTaskProcessors;
    }

    public void setTasks(IMap<TaskKey, Task> tasks) {
        this.tasks = tasks;
    }
}
