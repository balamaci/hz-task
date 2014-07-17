package ro.fortsoft.hztask.master;

import com.google.common.base.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ro.fortsoft.hztask.cluster.IClusterMasterService;
import ro.fortsoft.hztask.common.task.Task;
import ro.fortsoft.hztask.common.task.TaskKey;
import ro.fortsoft.hztask.master.distribution.ClusterDistributionService;
import ro.fortsoft.hztask.master.listener.TaskCompletionListener;
import ro.fortsoft.hztask.master.listener.TaskCompletionListenerFactory;
import ro.fortsoft.hztask.master.scheduler.UnassignedTasksReschedulerThread;

import java.io.Serializable;

/**
 * @author Serban Balamaci
 */
public class ClusterMasterServiceImpl implements IClusterMasterService {

    private MasterConfig masterConfig;

    private ClusterDistributionService clusterDistributionService;

    private Logger log = LoggerFactory.getLogger(ClusterMasterServiceImpl.class);

    private UnassignedTasksReschedulerThread unassignedTasksReschedulerThread;

    public ClusterMasterServiceImpl(MasterConfig masterConfig, ClusterDistributionService clusterDistributionService) {
        this.masterConfig = masterConfig;
        this.clusterDistributionService = clusterDistributionService;
        unassignedTasksReschedulerThread = new UnassignedTasksReschedulerThread(clusterDistributionService,
                masterConfig);
    }

    @Override
    public void handleFinishedTask(TaskKey taskKey, Serializable response) {
        log.info("Task with id {} finished", taskKey.getTaskId());
        Task task = clusterDistributionService.removeTask(taskKey);
        if(task == null) {
            log.error("Could not find task with id {}", taskKey.getTaskId());
            return;
        }

        Optional<TaskCompletionListener> finishedTaskProcessor = getProcessorForTaskClass(task);
        if (finishedTaskProcessor.isPresent()) {
            finishedTaskProcessor.get().onSuccess(task, response);
        }
    }

    @Override
    public void handleFailedTask(TaskKey taskKey, Throwable exception) {
        log.info("Task with id {} failed", taskKey.getTaskId());
        Task task = clusterDistributionService.removeTask(taskKey);
        if(task == null) {
            log.error("Could not find task with id {}", taskKey.getTaskId());
            return;
        }

        Optional<TaskCompletionListener> finishedTaskProcessor = getProcessorForTaskClass(task);
        if (finishedTaskProcessor.isPresent()) {
            finishedTaskProcessor.get().onFail(task, exception);
        }
    }

    private Optional<TaskCompletionListener> getProcessorForTaskClass(Task task) {
        TaskCompletionListenerFactory taskCompletionListenerFactory = masterConfig.
                getFinishedTaskListeners().get(task.getClass());

        if (taskCompletionListenerFactory != null) {
            TaskCompletionListener taskCompletionListener = taskCompletionListenerFactory.getObject();
            return Optional.fromNullable(taskCompletionListener);
        }

        return Optional.absent();
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
