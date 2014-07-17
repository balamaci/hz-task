package ro.fortsoft.hztask.master;

import ro.fortsoft.hztask.master.listener.TaskCompletionListenerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Serban Balamaci
 */
public class MasterConfig {

    private Map<Class, TaskCompletionListenerFactory> finishedTaskListeners = new HashMap<>();

    private long unassignedTaskReschedulerWaitTimeMs = 10000;

    public void registerFinishedTaskCompletionListenerFactory(Class taskClass,
                                                     TaskCompletionListenerFactory taskCompletionListenerFactory) {
        finishedTaskListeners.put(taskClass, taskCompletionListenerFactory);
    }

    public Map<Class, TaskCompletionListenerFactory> getFinishedTaskListeners() {
        return finishedTaskListeners;
    }

    public long getUnassignedTaskReschedulerWaitTimeMs() {
        return unassignedTaskReschedulerWaitTimeMs;
    }



}
