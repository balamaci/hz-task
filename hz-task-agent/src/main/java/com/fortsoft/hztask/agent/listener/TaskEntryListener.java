package com.fortsoft.hztask.agent.listener;

import com.fortsoft.hztask.agent.ClusterAgent;
import com.fortsoft.hztask.agent.processor.TaskProcessor;
import com.fortsoft.hztask.agent.processor.TaskProcessorFactory;
import com.fortsoft.hztask.common.task.Task;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;

/**
 *
 *
 * @author Serban Balamaci
 */
public class TaskEntryListener implements EntryListener {

    private ClusterAgent clusterAgent;

    public TaskEntryListener(ClusterAgent clusterAgent) {
        this.clusterAgent = clusterAgent;
    }

    @Override
    public void entryAdded(EntryEvent event) {
        Class taskClass = event.getValue().getClass();
        TaskProcessorFactory factory = clusterAgent.getProcessorRegistry().get(taskClass);
        TaskProcessor taskProcessor = factory.getObject();

        taskProcessor.setTask((Task) event.getValue());
        taskProcessor.doWork();
    }

    @Override
    public void entryRemoved(EntryEvent event) {

    }

    @Override
    public void entryUpdated(EntryEvent event) {

    }

    @Override
    public void entryEvicted(EntryEvent event) {

    }

}
