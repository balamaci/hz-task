package ro.fortsoft.hztask.agent.processor;

import ro.fortsoft.hztask.common.task.Task;

import java.io.Serializable;

/**
 * Base interface for implementing the processing logic for Tasks
 *
 * You create an new instance and can inject from the {@link TaskProcessorFactory} any dependencies
 *
 * @author Serban Balamaci
 */
public interface TaskProcessor<R extends Serializable, T extends Task> {

    /**
     * Method to override and implement what the task is supposed to do
     * @param task Task that holds the data
     * @return processing result
     */
    public R process(T task);

}
