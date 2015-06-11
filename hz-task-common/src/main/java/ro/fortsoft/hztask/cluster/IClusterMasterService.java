package ro.fortsoft.hztask.cluster;

import ro.fortsoft.hztask.common.task.TaskKey;

import java.io.Serializable;

/**
 * Interface through which the Agents, by sending AbstractMasterOps back to the Master get a reference
 * to the clusterMasterService on the Master side and can invoke the methods exposed
 * by this interface.
 *
 * @author Serban Balamaci
 */
public interface IClusterMasterService {

    /**
     * Method called by the agent to handle a successfully finished task
     * @param taskKey taskKey
     * @param response the response from the task
     * @param agentUuid the agent uuid that finished the task
     */
    void handleFinishedTask(TaskKey taskKey, Serializable response, String agentUuid);

    /**
     * Method called by the agent to handle a failed finished task
     * @param taskKey taskKey
     * @param exception the encountered exception
     * @param agentUuid the agent uuid that finished the task
     */
    void handleFailedTask(TaskKey taskKey, Throwable exception, String agentUuid);

}
