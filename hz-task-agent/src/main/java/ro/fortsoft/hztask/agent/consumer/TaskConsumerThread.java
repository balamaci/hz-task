package ro.fortsoft.hztask.agent.consumer;

import com.hazelcast.core.IMap;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.SqlPredicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ro.fortsoft.hztask.agent.ClusterAgentService;
import ro.fortsoft.hztask.agent.executor.TaskExecutionService;
import ro.fortsoft.hztask.agent.processor.TaskProcessor;
import ro.fortsoft.hztask.agent.processor.TaskProcessorFactory;
import ro.fortsoft.hztask.common.HzKeysConstants;
import ro.fortsoft.hztask.common.task.Task;
import ro.fortsoft.hztask.common.task.TaskKey;
import ro.fortsoft.hztask.comparator.PriorityAndOldestTaskComparator;

import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 *
 * This is the main thread that scans for new Tasks on the distribution list
 *
 * @author Serban Balamaci
 */
public class TaskConsumerThread extends Thread {

    /** BlockingQueue is Threadsafe **/
    private BlockingQueue<TaskKey> runningTasksQueue;

    private ClusterAgentService clusterAgentService;

    private IMap<TaskKey, Task> tasksMap;

    private TaskExecutionService taskExecutionService;

    private volatile boolean shuttingDown = false;

    private static final Logger log = LoggerFactory.getLogger(TaskConsumerThread.class);

    public TaskConsumerThread(ClusterAgentService clusterAgentService) {
        this.clusterAgentService = clusterAgentService;
        runningTasksQueue = new LinkedBlockingQueue<>(clusterAgentService.getMaxRunningTasks());
        tasksMap = clusterAgentService.getHzInstance().getMap(HzKeysConstants.TASKS_MAP);
        taskExecutionService = new TaskExecutionService(clusterAgentService.getEventBus());
    }

    @Override
    public void run() {
        log.info("Started TaskConsumer thread");

        String localClusterId = clusterAgentService.getHzInstance().getCluster().getLocalMember().getUuid();

        while (true) {
            if(shuttingDown) {
                break;
            }
            try {
                boolean foundTask = false;

                Set<TaskKey> eligibleTasks = retrieveTasksAssignedToInstanceId(localClusterId);

//                log.debug("Returned " + eligibleTasks.size() + " remaining " + runningTasksQueue.remainingCapacity());

                for (TaskKey taskKey : eligibleTasks) {
                    if (!runningTasksQueue.contains(taskKey)) {
                        Task task = tasksMap.get(taskKey);
                        if(task != null) { //The query ran before the task was removed from the map
                            foundTask = true;
                            startProcessingTask(taskKey, task);
                        }
                    }
                }
                if (!foundTask) {
                    log.debug("No Tasks found to process, sleeping a while...");
                    Thread.sleep(2000);
                }
            } catch (InterruptedException e) {
                log.info("TaskConsumer Thread received an interrupt signal, stopping");
                break;
            } catch (Throwable t) {
                log.info("TaskConsumer Thread encountered unexpected exception", t);
                break;
            }
        }
        log.info("TaskConsumer Thread terminated");
    }

    private Set<TaskKey> retrieveTasksAssignedToInstanceId(String localClusterId) {
        Predicate selectPredicate = new SqlPredicate("clusterInstanceUuid=" + localClusterId);

        PagingPredicate pagingPredicate = new PagingPredicate(selectPredicate,
                new PriorityAndOldestTaskComparator(), clusterAgentService.getMaxRunningTasks() + 1);

        return tasksMap.keySet(pagingPredicate);
    }

    private void startProcessingTask(TaskKey taskKey,Task task) throws InterruptedException {
        log.info("Starting processing of task {}", task);
        runningTasksQueue.put(taskKey);

        TaskProcessorFactory factory = clusterAgentService.getProcessorRegistry().get(task.getClass());
        TaskProcessor taskProcessor = factory.getObject();

        taskExecutionService.executeTask(taskProcessor, taskKey, task);
    }

    /**
     * Remove task from queue of runningTasks
     * @param taskKey taskKey
     */
    public void removeFromRunningTasksQueue(TaskKey taskKey) {
        runningTasksQueue.remove(taskKey);
    }

    public void outputDebugStatistics() {
    }

    public void shutDown() {
        shuttingDown = true;
        interrupt();
    }
}
