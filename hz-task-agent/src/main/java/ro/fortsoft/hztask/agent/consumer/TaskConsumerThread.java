package ro.fortsoft.hztask.agent.consumer;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.SqlPredicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ro.fortsoft.hztask.agent.ClusterAgentService;
import ro.fortsoft.hztask.agent.processor.TaskProcessor;
import ro.fortsoft.hztask.agent.processor.TaskProcessorFactory;
import ro.fortsoft.hztask.agent.service.TaskExecutionService;
import ro.fortsoft.hztask.op.NotifyMasterTaskFailedOp;
import ro.fortsoft.hztask.op.NotifyMasterTaskFinishedOp;
import ro.fortsoft.hztask.common.HzKeysConstants;
import ro.fortsoft.hztask.common.task.Task;
import ro.fortsoft.hztask.common.task.TaskKey;
import ro.fortsoft.hztask.comparator.PriorityAndOldestTaskComparator;
import ro.fortsoft.hztask.util.ClusterUtil;

import java.io.Serializable;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

/**
 *
 * This is the main thread that scans for new Tasks on the distribution list
 *
 * @author Serban Balamaci
 */
public class TaskConsumerThread extends Thread {

//    private Set<TaskKey> localTaskQueue;

    private BlockingQueue<TaskKey> runningTasks;

    private ClusterAgentService clusterAgentService;

    private static final Logger log = LoggerFactory.getLogger(TaskConsumerThread.class);

    private volatile boolean shuttingDown = false;

    private IMap<TaskKey, Task> tasksMap;

    private TaskExecutionService taskExecutionService;


    public TaskConsumerThread(ClusterAgentService clusterAgentService) {
        this.clusterAgentService = clusterAgentService;
        runningTasks = new LinkedBlockingQueue<>(clusterAgentService.getMaxRunningTasks());
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
//                log.info("Returned " + eligibleTasks.size() + " remaining " + runningTasks.remainingCapacity());
                boolean foundTask = false;

                Set<TaskKey> eligibleTasks = retrieveTasksAssignedToInstanceId(localClusterId);

                for (TaskKey taskKey : eligibleTasks) {
                    if (!runningTasks.contains(taskKey)) {
                        Task task = tasksMap.get(taskKey);
                        if(task != null) { //The query ran before the task was removed from the map
                            foundTask = true;
                            log.info("Starting processing of task {}", task);
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
        runningTasks.put(taskKey);

        TaskProcessorFactory factory = clusterAgentService.getProcessorRegistry().get(task.getClass());
        TaskProcessor taskProcessor = factory.getObject();

        taskExecutionService.executeTask(taskProcessor, taskKey, task);
    }

    public void notifyTaskFinished(TaskKey taskKey, Serializable result) {
        HazelcastInstance hzInstance = clusterAgentService.getHzInstance();
        IExecutorService executorService = hzInstance.getExecutorService(HzKeysConstants.
                EXECUTOR_SERVICE_FINISHED_TASKS);

        log.info("Notifying Master of task {} finished successfully", taskKey.getTaskId());
        Member master = clusterAgentService.getMaster();
        if(master != null) {
            Future masterNotified = executorService.submitToMember(new NotifyMasterTaskFinishedOp(taskKey, result,
                            ClusterUtil.getLocalMemberUuid(hzInstance)),
                    master);
            try {
                masterNotified.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                log.error("Task {}, failed to notify Master of it's status", e);
            }
        } else {
            log.info("Wanted to notify Master but Master left");
        }

        runningTasks.remove(taskKey);
    }

    public void notifyTaskFailed(TaskKey taskKey, Throwable exception) {
        HazelcastInstance hzInstance = clusterAgentService.getHzInstance();
        IExecutorService executorService = hzInstance.getExecutorService(HzKeysConstants.
                EXECUTOR_SERVICE_FINISHED_TASKS);

        Member master = clusterAgentService.getMaster();
        if(master != null) {
            Future masterNotified = executorService.submitToMember(new NotifyMasterTaskFailedOp(taskKey, exception,
                            ClusterUtil.getLocalMemberUuid(hzInstance)),
                    master);
            try {
                masterNotified.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                log.error("Task {}, failed to notify Master of it's status", e);
            }
        } else {
            log.info("Wanted to notify Master but Master left");
        }
        runningTasks.remove(taskKey);
    }

    public void setShuttingDown(boolean shuttingDown) {
        this.shuttingDown = shuttingDown;
    }
}
