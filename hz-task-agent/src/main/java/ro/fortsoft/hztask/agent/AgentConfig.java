package ro.fortsoft.hztask.agent;

import ro.fortsoft.hztask.agent.processor.TaskProcessorFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration class required for constructing a new Agent
 *
 * @author Serban Balamaci
 */
public class AgentConfig {

    private Map<Class, TaskProcessorFactory> processorRegistry = new HashMap<>();

    private String name;

    /** maximum simultaneous running tasks on Agent **/
    private int maxRunningTasks = 5;

    public void registerTaskProcessorFactory(Class taskClass, TaskProcessorFactory taskProcessorFactory) {
        processorRegistry.put(taskClass, taskProcessorFactory);
    }

    public Map<Class, TaskProcessorFactory> getProcessorRegistry() {
        return processorRegistry;
    }

    public int getMaxRunningTasks() {
        return maxRunningTasks;
    }

    public void setMaxRunningTasks(int maxRunningTasks) {
        this.maxRunningTasks = maxRunningTasks;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
