package ro.fortsoft.hztask.master.service;

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
import ro.fortsoft.hztask.master.router.RoundRobinRoutingStrategy;
import ro.fortsoft.hztask.master.router.RoutingStrategy;
import ro.fortsoft.hztask.master.scheduler.TasksDistributionThread;
import ro.fortsoft.hztask.master.statistics.IStatisticsService;
import ro.fortsoft.hztask.master.statistics.TaskTransitionLogKeeper;
import ro.fortsoft.hztask.master.topology.HazelcastTopologyService;
import ro.fortsoft.hztask.master.util.NamesUtil;

import java.util.Collection;
import java.util.Optional;
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

    private TaskTransitionLogKeeper taskTransitionLogKeeper;

    /**internal counter to keep track of the order of processed tasks**/
    private AtomicLong latestTaskCounter;

    private static final String LOCAL_MASTER_UUID = "-1";

    private volatile boolean shuttingDown = false;

    private static final Logger log = LoggerFactory.getLogger(ClusterDistributionService.class);

    /** the number of task remaining for an Agent before the Master restarts distributing tasks **/
    private static final int MIN_RUN_TASK_REMAINING = 10;

    public ClusterDistributionService(HazelcastTopologyService hazelcastTopologyService,
                                      IStatisticsService statisticsService) {
        this.hazelcastTopologyService = hazelcastTopologyService;
        this.statisticsService = statisticsService;

        this.routingStrategy = new RoundRobinRoutingStrategy(hazelcastTopologyService);
        this.tasks = hazelcastTopologyService.getHzInstance().getMap(HzKeysConstants.TASKS_MAP);
        tasksDistributionThread = new TasksDistributionThread(this);
    }

    /**
     * Add task for distribution to Agents
     * @param task Task
     */
    public void queueTask(Task task) {
        TaskKey taskKey = new TaskKey(task.getId());
        taskKey.setTaskNo(latestTaskCounter.getAndIncrement());

        task.setClusterInstanceUuid(LOCAL_MASTER_UUID);

        log.info("Adding task={} to Map", task);
        tasks.set(taskKey, task);

        taskTransitionLogKeeper.taskReceived(taskKey.getTaskId());

        startTaskDistributionThread();
    }

    private void rescheduleTask(TaskKey taskKey) {
        Task task = tasks.get(taskKey);

        String oldClusterInstanceAssignedToTask = task.getClusterInstanceUuid();
        String clusterInstanceId = getClusterInstanceToRunOn(task);
        task.setClusterInstanceUuid(clusterInstanceId);

        tasks.set(taskKey, task);

        if (!clusterInstanceId.equals(LOCAL_MASTER_UUID)) {
            if(oldClusterInstanceAssignedToTask.equals(LOCAL_MASTER_UUID)) {
                log.info("Assigning task={} to run on Agent {}", task, NamesUtil.
                        toLogFormat(task.getClusterInstanceUuid()));
                taskTransitionLogKeeper.taskAssigned(taskKey.getTaskId(), clusterInstanceId);
            } else {
                log.info("Rescheduling task={} to run on Agent {}", task, NamesUtil.
                        toLogFormat(task.getClusterInstanceUuid()));
                taskTransitionLogKeeper.taskReassigned(taskKey.getTaskId(), clusterInstanceId);
            }

            statisticsService.incSubmittedTasks(task.getTaskType(), clusterInstanceId);
            statisticsService.decUnassignedTask(task.getTaskType());

            taskTransitionLogKeeper.taskReassigned(taskKey.getTaskId(), clusterInstanceId);
        } else { //we're unassigning a task
            log.info("Unassigned task={}", task);

            statisticsService.incUnassignedTasks(task.getTaskType());
            taskTransitionLogKeeper.taskUnassigned(taskKey.getTaskId());
        }
    }

    private void unassignTask(TaskKey taskKey) {
        Task task = tasks.get(taskKey);

        if (!LOCAL_MASTER_UUID.equals(task.getClusterInstanceUuid())) {
            String clusterInstanceId = LOCAL_MASTER_UUID;

            task.setClusterInstanceUuid(clusterInstanceId);

            log.info("Unassigning task={}", task);
            tasks.set(taskKey, task);

            statisticsService.incUnassignedTasks(task.getTaskType());
        }
    }

    /**
     * Uses the RoutingStrategy to look for the next agentUuid where
     * @param task the task to run
     *
     * @return the agentUuid where the next task should run
     */
    private String getClusterInstanceToRunOn(Task task) {
        Optional<Member> memberToRunOn = routingStrategy.getMemberToRunOn(task);

        String executeOn = LOCAL_MASTER_UUID; //unassigned
        if (memberToRunOn.isPresent()) {
            executeOn = memberToRunOn.get().getUuid();
        }

        return executeOn;
    }

    public Task finishedTask(TaskKey taskKey, String agentUuid, boolean taskFailed) {
        Task task = tasks.remove(taskKey);
        if (taskFailed) {
            statisticsService.incTaskFailedCounter(task.getTaskType(), agentUuid);
            taskTransitionLogKeeper.taskFinishedFailure(taskKey.getTaskId());
        } else {
            statisticsService.incTaskFinishedCounter(task.getTaskType(), agentUuid);
            taskTransitionLogKeeper.taskFinishedSuccess(taskKey.getTaskId());
        }

        if(shouldReStartTaskDistributionThread(agentUuid, MIN_RUN_TASK_REMAINING)) {
            startTaskDistributionThread();
        }
        return task;
    }

    private boolean shouldReStartTaskDistributionThread(String agentUuid, int minRemainingTasks) {
        long totalSubmitted = statisticsService.getSubmittedTasks(agentUuid);
        long totalProcessed = statisticsService.getFinishedTasks(agentUuid)
                + statisticsService.getFailedTasks(agentUuid);

        long remaining = totalSubmitted - totalProcessed;
        log.info("{} Remaining tasks for {}", remaining, NamesUtil.toLogFormat(agentUuid));

        return remaining < minRemainingTasks;
    }

    public synchronized void startTaskDistributionThread() {
        if(shuttingDown) {
            return;
        }

        if (!tasksDistributionThread.isAlive()) {
            log.info("Starting up TaskDistributionThread");
            TasksDistributionThread newTaskDistributionThread = new TasksDistributionThread(this);
            newTaskDistributionThread.setLastThroughput(tasksDistributionThread.getLastThroughput());
            newTaskDistributionThread.setBatchSize(tasksDistributionThread.getBatchSize());

            tasksDistributionThread = newTaskDistributionThread;
            tasksDistributionThread.start();
        } else {
            log.info("TaskDistributionThread is still running...");
        }
    }


    private Collection<Task> queryTasks(Predicate predicate) {
        return tasks.values(predicate);
    }

    private Set<TaskKey> queryTaskKeys(Predicate predicate) {
        return tasks.keySet(predicate);
    }

    /**
     * Looks for tasks and unassignes them.
     * Useful in case the master went down but the tasks list is rebuilt from the active masters
     *
     * @param lastKey a key to reference when the master went back up so we don't reassign task
     *                that were submitted after the current master took control.
     */
    public void unassignOlderTasks(long lastKey) {
        Predicate oldTaskPredicate = Predicates.lessThan("internalCounter", lastKey);
        Predicate notAssigned = Predicates.notEqual("clusterInstanceUuid", LOCAL_MASTER_UUID);

        Predicate selectionPredicate = Predicates.and(oldTaskPredicate, notAssigned);
        int batchSize = 100;
        while (true) {
            boolean moreTasksFound = unassignMatchedTasks(batchSize, selectionPredicate);
            if(! moreTasksFound) {
                break;
            }
        }
    }

    /**
     * Reschedule an agents tasks - useful if the agent went down
     * @param agentUuid agentUuid
     */
    public void rescheduleAgentTasks(String agentUuid) {
        Predicate selectionPredicate = Predicates.equal("clusterInstanceUuid", agentUuid);

        while(true) {
            boolean moreTasksFound = rescheduleMatchedTasks(100, selectionPredicate);
            if(! moreTasksFound) {
                break;
            }
        }
    }

    /**
     * Reschedule unassigned tasks to agents
     * @param batchSize size of the reasigning batch
     * @return true if tasks unassigned were found
     */
    public boolean rescheduleUnassignedTasks(int batchSize) {
        Predicate selectionPredicate = Predicates.equal("clusterInstanceUuid", LOCAL_MASTER_UUID);
        return rescheduleMatchedTasks(batchSize, selectionPredicate);
    }

    /**
     *
     * @param batchSize
     * @param selectionPredicate
     * @return
     */
    private boolean unassignMatchedTasks(int batchSize, Predicate selectionPredicate) {
        PagingPredicate pagingPredicate = new PagingPredicate(selectionPredicate,
                new PriorityAndOldestTaskComparator(),
                batchSize);

        Set<TaskKey> foundTasks = queryTaskKeys(pagingPredicate);
        log.info("Looking for paged tasks matching {} found {} ", selectionPredicate, foundTasks.size());

        for(TaskKey taskKey: foundTasks) {
            unassignTask(taskKey);
        }

        return foundTasks.size() > 0;
    }

    private boolean rescheduleMatchedTasks(int batchSize, Predicate selectionPredicate) {

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

    public synchronized void shutdown() {
        shuttingDown = true;
        tasksDistributionThread.interrupt();
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
    
    public void setTaskTransitionLogKeeper(TaskTransitionLogKeeper taskTransitionLogKeeper) {
        this.taskTransitionLogKeeper = taskTransitionLogKeeper;
    }

    public boolean isShuttingDown() {
        return shuttingDown;
    }
}
