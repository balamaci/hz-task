package com.fortsoft.hztask.master;

import com.fortsoft.hztask.common.task.Task;
import com.fortsoft.hztask.master.distribution.ClusterDistributionService;
import com.fortsoft.hztask.master.listener.ClusterMembershipListener;
import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author Serban Balamaci
 */
public class ClusterMaster {

    private static final Logger log = LoggerFactory.getLogger(ClusterMaster.class);

    private HazelcastInstance hzInstance;

    private ClusterDistributionService clusterDistributionService;
    private HazelcastTopologyService hazelcastTopologyService;

    private volatile boolean shuttingDown = false;


    public ClusterMaster(MasterConfig masterConfig, String configXmlFileName) {
        Config cfg = new ClasspathXmlConfig(configXmlFileName);

        hzInstance = Hazelcast.newHazelcastInstance(cfg);
        hazelcastTopologyService = new HazelcastTopologyService(hzInstance);

        hzInstance.getCluster().addMembershipListener(new ClusterMembershipListener(hazelcastTopologyService));

        clusterDistributionService = new ClusterDistributionService(hazelcastTopologyService);
        ClusterMasterServiceImpl clusterMasterService = new ClusterMasterServiceImpl(masterConfig,
                clusterDistributionService);

        hzInstance.getConfig().getUserContext().put("clusterMasterService", clusterMasterService);
    }

    public void submitTask(Task task) {
        clusterDistributionService.submitDistributedTask(task);
    }

    public void shutdown() {
        shuttingDown = true;

        List<Member> members = hazelcastTopologyService.getAgents();
        for(Member member : members) {
            hazelcastTopologyService.sendShutdownMessageToMember(member);
        }

        hzInstance.shutdown();
    }

    public int getActiveWorkersCount() {
        return hazelcastTopologyService.getAgents().size();
    }


}
