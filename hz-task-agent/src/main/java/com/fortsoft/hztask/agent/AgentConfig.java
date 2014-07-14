package com.fortsoft.hztask.agent;

import com.fortsoft.hztask.agent.processor.TaskProcessorFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Serban Balamaci
 */
public class AgentConfig {

    private Map<Class, TaskProcessorFactory> processorRegistry = new HashMap<>();

    private int maxRunningTasks = 10;

    public void registerTaskProcessorFactory(Class taskClass, TaskProcessorFactory taskProcessorFactory) {
        processorRegistry.put(taskClass, taskProcessorFactory);
    }

    public Map<Class, TaskProcessorFactory> getProcessorRegistry() {
        return processorRegistry;
    }

    public int getMaxRunningTasks() {
        return maxRunningTasks;
    }


}
