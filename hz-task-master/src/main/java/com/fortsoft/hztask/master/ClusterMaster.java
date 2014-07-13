package com.fortsoft.hztask.master;

import com.fortsoft.hztask.cluster.IClusterMasterService;
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

import java.io.Serializable;

/**
 * @author Serban Balamaci
 */
public class ClusterMaster implements IClusterMasterService {

    private static final Logger log = LoggerFactory.getLogger(ClusterMaster.class);

    private HazelcastInstance hzInstance;

    private RoutingStrategy routingStrategy;

    private HazelcastTopologyService hazelcastTopologyService;

    private IMap<TaskKey, Task> tasks;


    public ClusterMaster(String configXmlFileName) {
        Config cfg = new ClasspathXmlConfig(configXmlFileName);

        hzInstance = Hazelcast.newHazelcastInstance(cfg);
        hazelcastTopologyService = new HazelcastTopologyService(hzInstance);
        routingStrategy = new RoundRobinRoutingStrategy(hazelcastTopologyService);

        hzInstance.getCluster().addMembershipListener(new ClusterMembershipListener(hazelcastTopologyService));
        hzInstance.getConfig().getUserContext().put("clusterMasterService", this);

        tasks = hzInstance.getMap("tasks");
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
        log.info("Adding task={} to Map for CLID {}", task.getId(), task.getClusterInstanceUuid());
        tasks.put(taskKey, task);
        log.info("Added task to Map", task.getId());
    }

    public int getActiveWorkersCount() {
        return hazelcastTopologyService.getAgents().size();
    }

    @Override
    public void handleFinishedTask(TaskKey taskKey, Serializable response) {
        tasks.remove(taskKey);
    }

    @Override
    public void handleFailedTask(TaskKey taskKey, Throwable exception) {
        tasks.remove(taskKey);
    }
}
