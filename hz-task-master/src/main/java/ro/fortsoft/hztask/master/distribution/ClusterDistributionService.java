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
import ro.fortsoft.hztask.master.scheduler.TasksDistributionThread;
import ro.fortsoft.hztask.master.statistics.IStatisticsService;

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

    private IStatisticsService statisticsService;

    private TasksDistributionThread tasksDistributionThread;

    //
    private AtomicLong latestTaskCounter;

    private static final String LOCAL_MASTER_UUID = "-1";

    private static final Logger log = LoggerFactory.getLogger(ClusterDistributionService.class);

    public ClusterDistributionService(HazelcastTopologyService hazelcastTopologyService,
                                      IStatisticsService statisticsService) {
        this.hazelcastTopologyService = hazelcastTopologyService;
        this.statisticsService = statisticsService;
        this.routingStrategy = new RoundRobinRoutingStrategy(hazelcastTopologyService);
        this.tasks = hazelcastTopologyService.getHzInstance().getMap(HzKeysConstants.TASKS_MAP);
        tasksDistributionThread = new TasksDistributionThread(this);
    }

    public void enqueueTask(Task task) {
        TaskKey taskKey = new TaskKey(task.getId());
        task.setClusterInstanceUuid(LOCAL_MASTER_UUID);
        task.setInternalCounter(latestTaskCounter.getAndIncrement());

        log.info("Adding task={} to Map", task);
        tasks.set(taskKey, task);

        statisticsService.incBacklogTask(task.getClass().getName());
    }

    public void submitDistributedTask(Task task) {
        String clusterInstanceId = getClusterInstanceToRunOn();
        TaskKey taskKey = new TaskKey(task.getId());
        task.setClusterInstanceUuid(clusterInstanceId);

        task.setInternalCounter(latestTaskCounter.getAndIncrement());

        log.info("Adding task={} to Map for AgentID {}", task, task.getClusterInstanceUuid());
        tasks.set(taskKey, task);
        log.info("Added task to Map", task.getId());

        statisticsService.decBacklogTask(task.getClass().getName());
        statisticsService.incSubmittedTasks(task.getClass().getName(), clusterInstanceId);
    }

    public void rescheduleTask(TaskKey taskKey) {
        Task task = tasks.get(taskKey);
        task.setInternalCounter(latestTaskCounter.getAndIncrement());

        String clusterInstanceId = getClusterInstanceToRunOn();
        task.setClusterInstanceUuid(clusterInstanceId);

        log.info("Rescheduling task={} to run on AgentID {}", task, task.getClusterInstanceUuid());
        tasks.set(taskKey, task);
        if(! clusterInstanceId.equals(LOCAL_MASTER_UUID)) {
            statisticsService.incSubmittedTasks(task.getClass().getName(), clusterInstanceId);
        }
    }

    private String getClusterInstanceToRunOn() {
        Optional<Member> memberToRunOn = routingStrategy.getMemberToRunOn();

        String executeOn = LOCAL_MASTER_UUID; //unassigned
        if (memberToRunOn.isPresent()) {
            executeOn = memberToRunOn.get().getUuid();
        }

        return executeOn;
    }

    public Task finishedTask(TaskKey taskKey, String agentUuid, boolean taskFailed) {
        Task task = tasks.remove(taskKey);
        if(taskFailed) {
            statisticsService.incTaskFailedCounter(task.getClass().getName(), agentUuid);
        } else {
            statisticsService.incTaskFinishedCounter(task.getClass().getName(), agentUuid);
        }

        doAfterTaskFinished(task, agentUuid);
        return task;
    }

    private void doAfterTaskFinished(Task task, String agentUuid) {
        long totalSubmitted = statisticsService.getSubmittedTotalTaskCount(agentUuid);
        long totalProcessed = statisticsService.getTaskFinishedCountForMember(agentUuid)
                + statisticsService.getTaskFailedCountForMember(agentUuid);

        long remaining = totalSubmitted - totalProcessed;
        log.info("Found remaining tasks {} for {}", remaining, agentUuid);

        if(remaining < 10) {
            startTaskDistributionThread();
        }
    }

    public synchronized void startTaskDistributionThread() {
        if(! tasksDistributionThread.isAlive()) {
            log.info("Starting up TaskDistributionThread");
            TasksDistributionThread newTaskDistributionThread = new TasksDistributionThread(this);
            newTaskDistributionThread.setLastThroughput(tasksDistributionThread.getLastThroughput());
            newTaskDistributionThread.setWindowSize(tasksDistributionThread.getWindowSize());

            tasksDistributionThread = newTaskDistributionThread;
            tasksDistributionThread.start();
        } else {
            log.info("TaskDistributionThread is already running...");
        }
    }

    public Collection<Task> queryTasks(Predicate predicate) {
        return tasks.values(predicate);
    }

    public Set<TaskKey> queryTaskKeys(Predicate predicate) {
        return tasks.keySet(predicate);
    }

/*
    public void rescheduleTask(TaskKey taskKey) {
        Task oldTask = removeTask(taskKey);
        submitDistributedTask(oldTask);
    }
*/

    public void unassignOlderTasks(long lastKey) {
        Predicate selectionPredicate = Predicates.lessThan("internalCounter", lastKey);

        for (; ; ) {
            boolean moreTasksFound = resetMatchedTasks(100, selectionPredicate);
            if(! moreTasksFound) {
                break;
            }
        }
    }

    public void rescheduleAgentTasks(String agentUuid) {
        Predicate selectionPredicate = Predicates.equal("clusterInstanceUuid", agentUuid);

        for (; ; ) {
            boolean moreTasksFound = rescheduleMatchedTasks(100, selectionPredicate);
            if(! moreTasksFound) {
                break;
            }
        }
    }

    public boolean rescheduleUnassignedTasks(int batchSize) {
        Predicate selectionPredicate = Predicates.equal("clusterInstanceUuid", LOCAL_MASTER_UUID);
        return rescheduleMatchedTasks(batchSize, selectionPredicate);
    }

    public boolean resetMatchedTasks(int batchSize, Predicate selectionPredicate) {
        PagingPredicate pagingPredicate = new PagingPredicate(selectionPredicate,
                new PriorityAndOldestTaskComparator(),
                batchSize);

        Set<TaskKey> foundTasks = queryTaskKeys(pagingPredicate);
        log.info("Looking for paged tasks matching {} found {} ", selectionPredicate, foundTasks.size());

        for(TaskKey taskKey: foundTasks) {
            rescheduleTask(taskKey);
        }

        return foundTasks.size() > 0;
    }

    public boolean rescheduleMatchedTasks(int batchSize, Predicate selectionPredicate) {

        PagingPredicate pagingPredicate = new PagingPredicate(selectionPredicate,
                new PriorityAndOldestTaskComparator(),
                batchSize);

        Set<TaskKey> foundTasks = queryTaskKeys(pagingPredicate);
        log.info("Looking for paged tasks matching {} found {} ", selectionPredicate, foundTasks.size());

        for(TaskKey taskKey: foundTasks) {
            rescheduleTask(taskKey);
        }

        return foundTasks.size() > 0;
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

    public IStatisticsService getStatisticsService() {
        return statisticsService;
    }

    public HazelcastTopologyService getHazelcastTopologyService() {
        return hazelcastTopologyService;
    }
}
