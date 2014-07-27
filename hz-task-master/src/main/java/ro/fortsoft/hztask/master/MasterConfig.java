package ro.fortsoft.hztask.master;

import ro.fortsoft.hztask.master.listener.TaskCompletionHandlerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Serban Balamaci
 */
public class MasterConfig {

    private TaskCompletionHandlerFactory defaultHandlerFactory;

    private Map<Class, TaskCompletionHandlerFactory> finishedTaskListeners = new HashMap<>();

    public void registerFinishedTaskCompletionListenerFactory(Class taskClass,
                                                     TaskCompletionHandlerFactory taskCompletionHandlerFactory) {
        finishedTaskListeners.put(taskClass, taskCompletionHandlerFactory);
    }

    public Map<Class, TaskCompletionHandlerFactory> getFinishedTaskListeners() {
        return finishedTaskListeners;
    }

    public void setDefaultHandlerFactory(TaskCompletionHandlerFactory defaultHandlerFactory) {
        this.defaultHandlerFactory = defaultHandlerFactory;
    }

    public TaskCompletionHandlerFactory getDefaultHandlerFactory() {
        return defaultHandlerFactory;
    }
}
