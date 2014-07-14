package com.fortsoft.hztask.master;

import com.fortsoft.hztask.common.task.Task;
import com.fortsoft.hztask.common.task.TaskKey;
import com.fortsoft.hztask.master.listener.ClusterMembershipListener;
import com.fortsoft.hztask.master.router.RoundRobinRoutingStrategy;
import com.fortsoft.hztask.master.router.RoutingStrategy;
import com.google.common.base.Optional;
import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
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

    private RoutingStrategy routingStrategy;

    private HazelcastTopologyService hazelcastTopologyService;

    private volatile boolean shuttingDown = false;

    private IMap<TaskKey, Task> tasks;


    public ClusterMaster(MasterConfig masterConfig, String configXmlFileName) {
        Config cfg = new ClasspathXmlConfig(configXmlFileName);

        hzInstance = Hazelcast.newHazelcastInstance(cfg);
        hazelcastTopologyService = new HazelcastTopologyService(hzInstance);
        routingStrategy = new RoundRobinRoutingStrategy(hazelcastTopologyService);

        hzInstance.getCluster().addMembershipListener(new ClusterMembershipListener(hazelcastTopologyService));

        tasks = hzInstance.getMap("tasks");
        ClusterMasterServiceImpl clusterMasterService = new ClusterMasterServiceImpl(masterConfig);
        clusterMasterService.setTasks(tasks);

        hzInstance.getConfig().getUserContext().put("clusterMasterService", clusterMasterService);

    }

    public void submitDistributedTask(Task task) {
        Optional<Member> memberToRunOn = routingStrategy.getMemberToRunOn();
        TaskKey taskKey;
        if(memberToRunOn.isPresent()) {
            taskKey = new TaskKey(memberToRunOn.get().getUuid(), task.getId(), task.getClass().getName());
        } else {
            taskKey = new TaskKey(hzInstance.getCluster().getLocalMember().getUuid(),
                    task.getId(), task.getClass().getName());
        }

        task.setClusterInstanceUuid(taskKey.getPartitionKey());
        log.info("Adding task={} to Map for AgentID {}", task.getId(), task.getClusterInstanceUuid());
        tasks.put(taskKey, task);
        log.info("Added task to Map", task.getId());
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
