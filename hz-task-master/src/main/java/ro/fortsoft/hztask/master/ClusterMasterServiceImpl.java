package ro.fortsoft.hztask.master;

import com.hazelcast.core.Member;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ro.fortsoft.hztask.cluster.IClusterMasterService;
import ro.fortsoft.hztask.common.task.Task;
import ro.fortsoft.hztask.common.task.TaskKey;
import ro.fortsoft.hztask.master.service.ClusterDistributionService;
import ro.fortsoft.hztask.master.service.CommunicationService;
import ro.fortsoft.hztask.master.service.TaskCompletionHandlerService;
import ro.fortsoft.hztask.master.util.NamesUtil;

import java.io.Serializable;
import java.util.Collection;

/**
 * @author Serban Balamaci
 */
public class ClusterMasterServiceImpl implements IClusterMasterService {

    private MasterConfig masterConfig;
    private ClusterDistributionService clusterDistributionService;
    private TaskCompletionHandlerService taskCompletionHandlerService;
    private CommunicationService communicationService;

    private volatile boolean shuttingDown = false;

    private Logger log = LoggerFactory.getLogger(ClusterMasterServiceImpl.class);

    public ClusterMasterServiceImpl(MasterConfig masterConfig,
                                    ClusterDistributionService clusterDistributionService,
                                    CommunicationService communicationService,
                                    TaskCompletionHandlerService taskCompletionHandlerService) {
        this.masterConfig = masterConfig;
        this.clusterDistributionService = clusterDistributionService;
        this.taskCompletionHandlerService = taskCompletionHandlerService;
        this.communicationService = communicationService;
    }

    @Override
    public void handleFinishedTask(TaskKey taskKey, Serializable response, String agentUuid) {
        log.info("Task with id {} finished on {}", taskKey.getTaskId(),
                NamesUtil.toLogFormat(agentUuid));
        Task task = clusterDistributionService.finishedTask(taskKey, agentUuid, false);

        taskCompletionHandlerService.onSuccess(task, response);
    }

    @Override
    public void handleFailedTask(TaskKey taskKey, Throwable exception, String agentUuid) {
        log.info("Task with id {} failed on {}", taskKey.getTaskId(),
                NamesUtil.toLogFormat(agentUuid));
        Task task = clusterDistributionService.finishedTask(taskKey, agentUuid, true);

        taskCompletionHandlerService.onFail(task, exception);
    }

    public void shutdown() {
        shuttingDown = true;

        clusterDistributionService.stop();
        HazelcastTopologyService hazelcastTopologyService = clusterDistributionService.getHazelcastTopologyService();
        Collection<Member> members = hazelcastTopologyService.getAgentsCopy();
        for(Member member : members) {
            communicationService.sendShutdownMessageToMember(member);
        }

        hazelcastTopologyService.getHzInstance().shutdown();
    }

}
