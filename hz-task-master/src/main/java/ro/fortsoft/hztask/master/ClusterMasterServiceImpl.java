package ro.fortsoft.hztask.master;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ro.fortsoft.hztask.cluster.IClusterMasterService;
import ro.fortsoft.hztask.common.task.Task;
import ro.fortsoft.hztask.common.task.TaskKey;
import ro.fortsoft.hztask.master.distribution.ClusterDistributionService;
import ro.fortsoft.hztask.master.scheduler.UnassignedTasksReschedulerThread;
import ro.fortsoft.hztask.master.service.TaskCompletionHandlerService;

import java.io.Serializable;

/**
 * @author Serban Balamaci
 */
public class ClusterMasterServiceImpl implements IClusterMasterService {

    private MasterConfig masterConfig;

    private ClusterDistributionService clusterDistributionService;

    private UnassignedTasksReschedulerThread unassignedTasksReschedulerThread;

    private TaskCompletionHandlerService taskCompletionHandlerService;



    private Logger log = LoggerFactory.getLogger(ClusterMasterServiceImpl.class);

    public ClusterMasterServiceImpl(MasterConfig masterConfig,
                                    ClusterDistributionService clusterDistributionService,
                                    TaskCompletionHandlerService taskCompletionHandlerService) {
        this.masterConfig = masterConfig;
        this.clusterDistributionService = clusterDistributionService;
        this.taskCompletionHandlerService = taskCompletionHandlerService;
        unassignedTasksReschedulerThread = new UnassignedTasksReschedulerThread(clusterDistributionService,
                masterConfig);
    }

    @Override
    public void handleFinishedTask(TaskKey taskKey, Serializable response, String agentUuid) {
        log.info("Task with id {} finished on {}", taskKey.getTaskId(), agentUuid);
        Task task = clusterDistributionService.finishedTask(taskKey, agentUuid, false);

        taskCompletionHandlerService.onSuccess(task, response);
    }

    @Override
    public void handleFailedTask(TaskKey taskKey, Throwable exception, String agentUuid) {
        log.info("Task with id {} failed on {}", taskKey.getTaskId(), agentUuid);
        Task task = clusterDistributionService.finishedTask(taskKey, agentUuid, true);

        taskCompletionHandlerService.onFail(task, exception);
    }


    public synchronized void startUnassignedTasksReschedulerThread() {
        if(unassignedTasksReschedulerThread == null || ! unassignedTasksReschedulerThread.isAlive()) {
            unassignedTasksReschedulerThread = new UnassignedTasksReschedulerThread(clusterDistributionService,
                    masterConfig);
            unassignedTasksReschedulerThread.start();
        }
    }

    public synchronized void stopUnassignedTasksReschedulerThread() {
        if(unassignedTasksReschedulerThread != null && unassignedTasksReschedulerThread.isAlive()) {
            unassignedTasksReschedulerThread.interrupt();
        }
    }

}
