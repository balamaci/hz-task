package ro.fortsoft.hztask.master;

import ro.fortsoft.hztask.master.listener.TaskCompletionHandlerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Serban Balamaci
 */
public class MasterConfig {

    private Map<Class, TaskCompletionHandlerFactory> finishedTaskListeners = new HashMap<>();

    private long unassignedTaskReschedulerWaitTimeMs = 10000;

    public void registerFinishedTaskCompletionListenerFactory(Class taskClass,
                                                     TaskCompletionHandlerFactory taskCompletionHandlerFactory) {
        finishedTaskListeners.put(taskClass, taskCompletionHandlerFactory);
    }

    public Map<Class, TaskCompletionHandlerFactory> getFinishedTaskListeners() {
        return finishedTaskListeners;
    }

    public long getUnassignedTaskReschedulerWaitTimeMs() {
        return unassignedTaskReschedulerWaitTimeMs;
    }



}
