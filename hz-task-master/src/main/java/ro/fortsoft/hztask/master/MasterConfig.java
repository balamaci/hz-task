package ro.fortsoft.hztask.master;

import ro.fortsoft.hztask.master.listener.TaskCompletionHandlerFactory;
import ro.fortsoft.hztask.master.router.RoutingStrategy;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Serban Balamaci
 */
public class MasterConfig {

    private RoutingStrategy.Type routingStrategy = RoutingStrategy.Type.BALANCED_LOW_FAILURE_ORIENTED;

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

    public void setRoutingStrategy(RoutingStrategy.Type routingStrategy) {
        this.routingStrategy = routingStrategy;
    }

    public RoutingStrategy.Type getRoutingStrategy() {
        return routingStrategy;
    }
}
