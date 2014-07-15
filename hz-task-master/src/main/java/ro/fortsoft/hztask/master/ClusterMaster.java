package ro.fortsoft.hztask.master;

import ro.fortsoft.hztask.common.task.Task;
import ro.fortsoft.hztask.master.distribution.ClusterDistributionService;
import ro.fortsoft.hztask.master.listener.ClusterMembershipListener;
import ro.fortsoft.hztask.master.scheduler.UnassignedTaskReschedulerThread;
import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

/**
 * @author Serban Balamaci
 */
public class ClusterMaster {

    private static final Logger log = LoggerFactory.getLogger(ClusterMaster.class);

    private HazelcastInstance hzInstance;

    private ClusterDistributionService clusterDistributionService;
    private HazelcastTopologyService hazelcastTopologyService;

    private UnassignedTaskReschedulerThread unassignedTaskReschedulerThread;

    private volatile boolean shuttingDown = false;


    public ClusterMaster(MasterConfig masterConfig, String configXmlFileName) {
        Config cfg = new ClasspathXmlConfig(configXmlFileName);

        hzInstance = Hazelcast.newHazelcastInstance(cfg);
        hazelcastTopologyService = new HazelcastTopologyService(hzInstance);

        Set<Member> memberSet = hzInstance.getCluster().getMembers();
        for(Member member : memberSet) {
            if(! member.localMember()) {
                hazelcastTopologyService.callbackWhenAgentReady(member, 0);
            }
        }

        hzInstance.getCluster().addMembershipListener(new ClusterMembershipListener(hazelcastTopologyService));

        clusterDistributionService = new ClusterDistributionService(hazelcastTopologyService);
        if(clusterDistributionService.getTaskCount() > 0) {

        }

        ClusterMasterServiceImpl clusterMasterService = new ClusterMasterServiceImpl(masterConfig,
                clusterDistributionService);

        hzInstance.getConfig().getUserContext().put("clusterMasterService", clusterMasterService);
        unassignedTaskReschedulerThread = new UnassignedTaskReschedulerThread(clusterMasterService, 10000);
        unassignedTaskReschedulerThread.start();
    }

    public void submitTask(Task task) {
        clusterDistributionService.submitDistributedTask(task);
    }

    public void shutdown() {
        shuttingDown = true;

        unassignedTaskReschedulerThread.interrupt();
        List<Member> members = hazelcastTopologyService.getAgents();
        for(Member member : members) {
            hazelcastTopologyService.sendShutdownMessageToMember(member);
        }

        hzInstance.shutdown();
    }

    public int getActiveWorkersCount() {
        return hazelcastTopologyService.getAgentsCount();
    }


}
