package ro.fortsoft.hztask.master;

import ro.fortsoft.hztask.cluster.IClusterMasterService;
import ro.fortsoft.hztask.common.task.Task;
import ro.fortsoft.hztask.common.task.TaskKey;
import ro.fortsoft.hztask.master.distribution.ClusterDistributionService;
import ro.fortsoft.hztask.master.processor.TaskCompletionListener;
import ro.fortsoft.hztask.master.processor.TaskCompletionListenerFactory;
import com.google.common.base.Optional;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Set;

/**
 * @author Serban Balamaci
 */
public class ClusterMasterServiceImpl implements IClusterMasterService {

    private MasterConfig masterConfig;

    private ClusterDistributionService clusterDistributionService;

    private Logger log = LoggerFactory.getLogger(ClusterMasterServiceImpl.class);

    public ClusterMasterServiceImpl(MasterConfig masterConfig, ClusterDistributionService clusterDistributionService) {
        this.masterConfig = masterConfig;
        this.clusterDistributionService = clusterDistributionService;
    }

    @Override
    public void handleFinishedTask(TaskKey taskKey, Serializable response) {
        log.info("Task with id {} finished", taskKey.getTaskId());
        Task task = clusterDistributionService.removeTask(taskKey);

        Optional<TaskCompletionListener> finishedTaskProcessor = getProcessorForTaskClass(task);
        if(finishedTaskProcessor.isPresent()) {
            finishedTaskProcessor.get().onSuccess(task, response);
        }
    }

    @Override
    public void handleFailedTask(TaskKey taskKey, Throwable exception) {
        log.info("Task with id {} failed", taskKey.getTaskId());
        Task task = clusterDistributionService.removeTask(taskKey);

        Optional<TaskCompletionListener> finishedTaskProcessor = getProcessorForTaskClass(task);
        if(finishedTaskProcessor.isPresent()) {
            finishedTaskProcessor.get().onFail(task, exception);
        }
    }

    private Optional<TaskCompletionListener> getProcessorForTaskClass(Task task) {
        TaskCompletionListenerFactory taskCompletionListenerFactory = masterConfig.
                getFinishedTaskProcessors().get(task.getClass());

        if(taskCompletionListenerFactory != null) {
            TaskCompletionListener taskCompletionListener = taskCompletionListenerFactory.getObject();
            return Optional.fromNullable(taskCompletionListener);
        }

        return Optional.absent();
    }

    public void rescheduleUnassignedTasks() {

        if(clusterDistributionService.getTaskCount() == 0) {
            log.info("No tasks to redistribute");
            return;
        }

        if(clusterDistributionService.getAgentsCount() ==0) {
            log.info("No Agents to redistribute task to");
            return;
        }

        Predicate selectionPredicate = Predicates.equal("clusterInstanceUuid", "-1");

        PagingPredicate pagingPredicate = new PagingPredicate(selectionPredicate, 100);

        Set<TaskKey> unscheduledTasks = clusterDistributionService.queryTaskKeys(pagingPredicate);
        log.info("Looking for unscheduled tasks found {} ", unscheduledTasks.size());

        for(TaskKey taskKey: unscheduledTasks) {
            clusterDistributionService.rescheduleTask(taskKey);
        }
    }

}
