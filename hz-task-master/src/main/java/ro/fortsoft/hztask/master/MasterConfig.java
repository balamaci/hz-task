package ro.fortsoft.hztask.master;

import ro.fortsoft.hztask.master.processor.TaskCompletionListenerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Serban Balamaci
 */
public class MasterConfig {

    private Map<Class, TaskCompletionListenerFactory> finishedTaskProcessors = new HashMap<>();

    public void registerFinishedTaskProcessorFactory(Class taskClass,
                                                     TaskCompletionListenerFactory taskCompletionListenerFactory) {
        finishedTaskProcessors.put(taskClass, taskCompletionListenerFactory);
    }

    public Map<Class, TaskCompletionListenerFactory> getFinishedTaskProcessors() {
        return finishedTaskProcessors;
    }
}
