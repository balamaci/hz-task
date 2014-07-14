package com.fortsoft.hztask.master;

import com.fortsoft.hztask.cluster.IClusterMasterService;
import com.fortsoft.hztask.common.task.Task;
import com.fortsoft.hztask.common.task.TaskKey;
import com.fortsoft.hztask.master.distribution.ClusterDistributionService;
import com.fortsoft.hztask.master.processor.FinishedTaskProcessor;
import com.fortsoft.hztask.master.processor.FinishedTaskProcessorFactory;
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
        Task task = clusterDistributionService.removeTask(taskKey);

        Optional<FinishedTaskProcessor> finishedTaskProcessor = getProcessorForTaskClass(task);
        if(finishedTaskProcessor.isPresent()) {
            finishedTaskProcessor.get().processSuccessful(task, response);
        }
    }

    @Override
    public void handleFailedTask(TaskKey taskKey, Throwable exception) {
        Task task = clusterDistributionService.removeTask(taskKey);

        Optional<FinishedTaskProcessor> finishedTaskProcessor = getProcessorForTaskClass(task);
        if(finishedTaskProcessor.isPresent()) {
            finishedTaskProcessor.get().processFailed(task, exception);
        }
    }

    private Optional<FinishedTaskProcessor> getProcessorForTaskClass(Task task) {
        FinishedTaskProcessorFactory finishedTaskProcessorFactory = masterConfig.
                getFinishedTaskProcessors().get(task.getClass());

        if(finishedTaskProcessorFactory != null) {
            FinishedTaskProcessor finishedTaskProcessor = finishedTaskProcessorFactory.getObject();
            return Optional.fromNullable(finishedTaskProcessor);
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
