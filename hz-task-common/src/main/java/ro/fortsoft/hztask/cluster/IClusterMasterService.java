package ro.fortsoft.hztask.cluster;

import ro.fortsoft.hztask.common.task.TaskKey;

import java.io.Serializable;

/**
 * Interface through which the Agents by sending AbstractMasterOps to the ClusterMaster get a reference
 * to the clusterMasterService which is exposed on the ClusterMaster and can call the methods exposed
 * by this interface.
 *
 * @author Serban Balamaci
 */
public interface IClusterMasterService {

    /**
     * Method called by the agent to handle a successfully finished task
     * @param taskKey taskKey
     * @param response the response from the task
     */
    void handleFinishedTask(TaskKey taskKey, Serializable response);

    public void handleFailedTask(TaskKey taskKey, Throwable exception);

}
