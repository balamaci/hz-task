package ro.fortsoft.hztask.master.distribution;

import ro.fortsoft.hztask.common.task.Task;
import ro.fortsoft.hztask.common.task.TaskKey;
import ro.fortsoft.hztask.master.HazelcastTopologyService;
import ro.fortsoft.hztask.master.router.RoundRobinRoutingStrategy;
import ro.fortsoft.hztask.master.router.RoutingStrategy;
import com.google.common.base.Optional;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.query.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Set;

/**
 * Handles task distribution to the Agents and general tasks management
 *
 * @author Serban Balamaci
 */
public class ClusterDistributionService {

    private RoutingStrategy routingStrategy;

    private HazelcastTopologyService hazelcastTopologyService;

    private IMap<TaskKey, Task> tasks;

    private static final Logger log = LoggerFactory.getLogger(ClusterDistributionService.class);

    public ClusterDistributionService(HazelcastTopologyService hazelcastTopologyService) {
        this.hazelcastTopologyService = hazelcastTopologyService;
        this.routingStrategy = new RoundRobinRoutingStrategy(hazelcastTopologyService);
        this.tasks = hazelcastTopologyService.getHzInstance().getMap("tasks");
    }

    public void submitDistributedTask(Task task) {
        Optional<Member> memberToRunOn = routingStrategy.getMemberToRunOn();
        TaskKey taskKey;
        if(memberToRunOn.isPresent()) {
            taskKey = new TaskKey(memberToRunOn.get().getUuid(), task.getId(), task.getClass().getName());
        } else {
            taskKey = new TaskKey("-1", task.getId(), task.getClass().getName());
        }

        task.setClusterInstanceUuid(taskKey.getPartitionKey());
        log.info("Adding task={} to Map for AgentID {}", task.getId(), task.getClusterInstanceUuid());
        tasks.put(taskKey, task);
        log.info("Added task to Map", task.getId());
    }

    public Task removeTask(TaskKey taskKey) {
        return tasks.remove(taskKey);
    }

    public Collection<Task> queryTasks(Predicate predicate) {
        return tasks.values(predicate);
    }

    public Set<TaskKey> queryTaskKeys(Predicate predicate) {
        return tasks.keySet(predicate);
    }

    public void rescheduleTask(TaskKey taskKey) {
        Task oldTask = removeTask(taskKey);
        submitDistributedTask(oldTask);
    }

    public int getTaskCount() {
        return tasks.size();
    }

    public int getAgentsCount() {
        return hazelcastTopologyService.getAgentsCount();
    }

    public void setRoutingStrategy(RoutingStrategy routingStrategy) {
        this.routingStrategy = routingStrategy;
    }
}
