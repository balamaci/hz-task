package ro.fortsoft.hztask.agent.processor;

import ro.fortsoft.hztask.common.task.Task;

import java.io.Serializable;

/**
 * Base class for processors of task to implement
 *
 * Override this class and inject from the Factory the dependencies
 *
 * @author Serban Balamaci
 */
public abstract class TaskProcessor<T extends Serializable> {

    /**
     * Method to override and implement what the task is supposed to do
     * @param task Task that holds the data
     * @return processing result
     */
    public abstract T process(Task task);

}
