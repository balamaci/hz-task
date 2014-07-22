package ro.fortsoft.hztask.master;

import com.google.common.eventbus.AsyncEventBus;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ro.fortsoft.hztask.common.HzKeysConstants;
import ro.fortsoft.hztask.common.MemberType;
import ro.fortsoft.hztask.common.task.Task;
import ro.fortsoft.hztask.master.event.membership.AgentMembershipSubscriber;
import ro.fortsoft.hztask.master.listener.ClusterMembershipListener;
import ro.fortsoft.hztask.master.router.BalancedWorkloadRoutingStrategy;
import ro.fortsoft.hztask.master.service.ClusterDistributionService;
import ro.fortsoft.hztask.master.service.CommunicationService;
import ro.fortsoft.hztask.master.service.TaskCompletionHandlerService;
import ro.fortsoft.hztask.master.statistics.CodahaleStatisticsService;

import java.util.Set;
import java.util.concurrent.Executors;

/**
 * @author Serban Balamaci
 */
public class ClusterMaster {

    private static final Logger log = LoggerFactory.getLogger(ClusterMaster.class);

    private HazelcastInstance hzInstance;

    private ClusterDistributionService clusterDistributionService;
    private HazelcastTopologyService hazelcastTopologyService;

    private AsyncEventBus eventBus = new AsyncEventBus(Executors.newCachedThreadPool());

    private ClusterMasterServiceImpl clusterMasterService;


    public ClusterMaster(MasterConfig masterConfig, Config hazelcastConfig) {
        hzInstance = Hazelcast.newHazelcastInstance(hazelcastConfig);

        hzInstance.getUserContext().put(HzKeysConstants.USER_CONTEXT_MEMBER_TYPE, MemberType.MASTER);

        CommunicationService communicationService = new CommunicationService(hzInstance);
        hazelcastTopologyService = new HazelcastTopologyService(hzInstance, eventBus, communicationService);
        clusterDistributionService = new ClusterDistributionService(hazelcastTopologyService,
                new CodahaleStatisticsService());
        clusterDistributionService.setRoutingStrategy(new BalancedWorkloadRoutingStrategy(hazelcastTopologyService,
                clusterDistributionService.getStatisticsService()));

        checkNoOtherMasterClusterAmongMembers();

        registerMembershipListener();

        registerAlreadyPresentAgents();
        hzInstance.getCluster().addMembershipListener(new ClusterMembershipListener(eventBus));

        //TODO with HZ3.3 change it to highest value retrieved by Aggregate
        long latestTaskCounter = System.currentTimeMillis();
        clusterDistributionService.setLatestTaskCounter(System.currentTimeMillis());
        clusterDistributionService.unassignOlderTasks(latestTaskCounter);

        TaskCompletionHandlerService taskCompletionHandlerService = new TaskCompletionHandlerService(masterConfig);
        clusterMasterService = new ClusterMasterServiceImpl(masterConfig,
                clusterDistributionService, communicationService, taskCompletionHandlerService);

        hzInstance.getUserContext().put(HzKeysConstants.USER_CONTEXT_CLUSTER_MASTER_SERVICE,
                clusterMasterService);
//        clusterMasterService.startUnassignedTasksReschedulerThread();
    }

    private void registerMembershipListener() {
        AgentMembershipSubscriber agentMembershipSubscriber =
                new AgentMembershipSubscriber(clusterDistributionService, hazelcastTopologyService);
        eventBus.register(agentMembershipSubscriber);
    }


    public void submitTask(Task task) {
        clusterDistributionService.enqueueTask(task);
    }

    private void checkNoOtherMasterClusterAmongMembers() {
        if(hazelcastTopologyService.isMasterAmongClusterMembers()) {
            hzInstance.shutdown();
            throw new RuntimeException("Another Master is already started");
        }
    }

    public void shutdown() {
        clusterMasterService.shutdown();
    }

    public int getActiveWorkersCount() {
        return hazelcastTopologyService.getAgentsCount();
    }

    //Master might be started after some Agent
    private void registerAlreadyPresentAgents() {
        Set<Member> memberSet = hzInstance.getCluster().getMembers();
        for(Member member : memberSet) {
            if(! member.localMember()) {
                hazelcastTopologyService.callbackWhenAgentReady(member, 0);
            }
        }
    }

}
