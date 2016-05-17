package ro.fortsoft.hztask.agent.consumer;

import com.hazelcast.core.IMap;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.SqlPredicate;
import javaslang.Tuple;
import javaslang.Tuple2;
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

import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

/**
 *
 * This is the main thread that scans for new Tasks on the distribution list
 *
 * @author Serban Balamaci
 */
public class TaskConsumerThread extends Thread {

    /** Queue that keeps the current tasks running - so that between ACK from Master that
     * it received the finished task, we do not start processing it again on the Agent**/
    private BlockingQueue<TaskKey> runningTasksQueue; //BlockingQueue is Threadsafe

    private ClusterAgentService clusterAgentService;

    private IMap<TaskKey, Task> tasksMap;

    private TaskExecutionService taskExecutionService;

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
            if(isInterrupted()) {
                break;
            }

            try {
                Set<TaskKey> eligibleTaskKey = retrieveTasksAssignedToInstanceIdOrdered(localClusterId);
                List<Tuple2<TaskKey, Task>> eligibleTask = filterAlreadyRunningTasks(eligibleTaskKey);

                log.debug("Returned {} processingNow={}", eligibleTask.size(), runningTasksQueue.size());

                for(Tuple2<TaskKey, Task> taskTuple : eligibleTask ) {
                    startProcessingTask(taskTuple._1, taskTuple._2);
                }

                if (eligibleTask.isEmpty()) {
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

    private List<Tuple2<TaskKey, Task>> filterAlreadyRunningTasks(Set<TaskKey> eligibleTasks ) {

        return eligibleTasks.stream()
                .filter((taskKey) -> !runningTasksQueue.contains(taskKey))
                .map((taskKey) -> Tuple.of(taskKey, tasksMap.get(taskKey)))
                .filter(tuple2 -> tuple2._2 != null)  //the Master could have removed the Task from the Map and that
                .collect(Collectors.toList());        //message to arrive faster, before he sent the ACK
    }

    //TODO wasteful to run the query probably considering the already running tasks are
    //not filtered out somehow
    private Set<TaskKey> retrieveTasksAssignedToInstanceIdOrdered(String localClusterId) {
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
        interrupt();
    }
}
