package com.fortsoft.hztask.master;

import com.fortsoft.hztask.master.processor.FinishedTaskProcessorFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Serban Balamaci
 */
public class MasterConfig {

    private Map<Class, FinishedTaskProcessorFactory> finishedTaskProcessors = new HashMap<>();

    public void registerFinishedTaskProcessorFactory(Class taskClass,
                                                     FinishedTaskProcessorFactory finishedTaskProcessorFactory) {
        finishedTaskProcessors.put(taskClass, finishedTaskProcessorFactory);
    }

    public Map<Class, FinishedTaskProcessorFactory> getFinishedTaskProcessors() {
        return finishedTaskProcessors;
    }
}
