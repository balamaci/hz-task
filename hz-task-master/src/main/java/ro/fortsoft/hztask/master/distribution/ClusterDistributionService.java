package ro.fortsoft.hztask.master.distribution;

import com.google.common.base.Optional;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ro.fortsoft.hztask.common.HzKeysConstants;
import ro.fortsoft.hztask.common.task.Task;
import ro.fortsoft.hztask.common.task.TaskKey;
import ro.fortsoft.hztask.comparator.PriorityAndOldestTaskComparator;
import ro.fortsoft.hztask.master.HazelcastTopologyService;
import ro.fortsoft.hztask.master.router.RoundRobinRoutingStrategy;
import ro.fortsoft.hztask.master.router.RoutingStrategy;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Handles task distribution to the Agents and general tasks management
 *
 * @author Serban Balamaci
 */
public class ClusterDistributionService {

    private RoutingStrategy routingStrategy;

    private HazelcastTopologyService hazelcastTopologyService;

    private IMap<TaskKey, Task> tasks;

    //
    private AtomicLong latestTaskCounter;

    private static final Logger log = LoggerFactory.getLogger(ClusterDistributionService.class);

    public ClusterDistributionService(HazelcastTopologyService hazelcastTopologyService) {
        this.hazelcastTopologyService = hazelcastTopologyService;
        this.routingStrategy = new RoundRobinRoutingStrategy(hazelcastTopologyService);
        this.tasks = hazelcastTopologyService.getHzInstance().getMap(HzKeysConstants.TASKS_MAP);
    }

    public void submitDistributedTask(Task task) {
        Optional<Member> memberToRunOn = routingStrategy.getMemberToRunOn();

        String executeOn = "-1"; //unassigned
        TaskKey taskKey = new TaskKey(task.getId());
        if (memberToRunOn.isPresent()) {
            executeOn = memberToRunOn.get().getUuid();
        }
        task.setClusterInstanceUuid(executeOn);

        task.setInternalCounter(latestTaskCounter.getAndIncrement());

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

    public void rescheduleOlderTasks(long lastKey) {
        Predicate selectionPredicate = Predicates.lessThan("internalCounter", lastKey);

        PagingPredicate pagingPredicate = new PagingPredicate(selectionPredicate,
                new PriorityAndOldestTaskComparator(),
                100);

        for( ; ; ){
            Set<TaskKey> oldTasks = queryTaskKeys(pagingPredicate);
            log.info("Looking for Older tasks, found {} ", oldTasks.size());

            for (TaskKey taskKey : oldTasks) {
                rescheduleTask(taskKey);
            }

            if(oldTasks.size() == 0) {
                break;
            }
        }
    }

    public boolean rescheduleUnassignedTasks(int batchSize) {
        Predicate selectionPredicate = Predicates.equal("clusterInstanceUuid", "-1");

        PagingPredicate pagingPredicate = new PagingPredicate(selectionPredicate,
                new PriorityAndOldestTaskComparator(),
                batchSize);

        Set<TaskKey> unscheduledTasks = queryTaskKeys(pagingPredicate);
        log.info("Looking for unscheduled tasks found {} ", unscheduledTasks.size());

        for(TaskKey taskKey: unscheduledTasks) {
            rescheduleTask(taskKey);
        }

        return unscheduledTasks.size() > 0;
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

    public void setLatestTaskCounter(long latestTaskCounter) {
        this.latestTaskCounter = new AtomicLong(latestTaskCounter);
    }
}
