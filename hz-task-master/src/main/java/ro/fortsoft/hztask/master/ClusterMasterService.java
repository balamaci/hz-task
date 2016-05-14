package ro.fortsoft.hztask.master;

import com.hazelcast.core.Member;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ro.fortsoft.hztask.cluster.IClusterMasterService;
import ro.fortsoft.hztask.common.task.Task;
import ro.fortsoft.hztask.common.task.TaskKey;
import ro.fortsoft.hztask.master.service.ClusterDistributionService;
import ro.fortsoft.hztask.master.service.CommunicationService;
import ro.fortsoft.hztask.master.service.TaskCompletionHandlerProvider;
import ro.fortsoft.hztask.master.topology.HazelcastTopologyService;
import ro.fortsoft.hztask.master.util.NamesUtil;

import java.io.Serializable;
import java.util.Collection;

/**
 * @author Serban Balamaci
 */
public class ClusterMasterService implements IClusterMasterService {

    private ClusterDistributionService clusterDistributionService;
    private TaskCompletionHandlerProvider taskCompletionHandlerProvider;
    private CommunicationService communicationService;

    private Logger log = LoggerFactory.getLogger(ClusterMasterService.class);

    public ClusterMasterService(ClusterDistributionService clusterDistributionService,
                                CommunicationService communicationService,
                                TaskCompletionHandlerProvider taskCompletionHandlerProvider) {
        this.clusterDistributionService = clusterDistributionService;
        this.taskCompletionHandlerProvider = taskCompletionHandlerProvider;
        this.communicationService = communicationService;
    }

    @Override
    public void handleFinishedTask(TaskKey taskKey, Serializable response, String agentUuid) {
        log.info("Task with id {} finished on {}", taskKey.getTaskId(),
                NamesUtil.toLogFormat(agentUuid));
        Task task = clusterDistributionService.finishedTask(taskKey, agentUuid, false);

        taskCompletionHandlerProvider.onSuccess(task, response);
    }

    @Override
    public void handleFailedTask(TaskKey taskKey, Throwable exception, String agentUuid) {
        log.info("Task with id {} failed on {}", taskKey.getTaskId(),
                NamesUtil.toLogFormat(agentUuid));
        Task task = clusterDistributionService.finishedTask(taskKey, agentUuid, true);

        taskCompletionHandlerProvider.onFail(task, exception);
    }

    /**
     * shutdown master and agents
     */
    public void shutdown() {
        clusterDistributionService.shutdown();
        HazelcastTopologyService hazelcastTopologyService = clusterDistributionService.getHazelcastTopologyService();
        Collection<Member> members = hazelcastTopologyService.getAgentsCopy();
        for(Member member : members) {
            communicationService.sendShutdownMessageToMember(member);
        }

        hazelcastTopologyService.getHzInstance().shutdown();
    }

}
