package ro.fortsoft.hztask.agent.consumer;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.IMap;
import com.hazelcast.query.SqlPredicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ro.fortsoft.hztask.agent.ClusterAgentServiceImpl;
import ro.fortsoft.hztask.agent.processor.TaskProcessor;
import ro.fortsoft.hztask.agent.processor.TaskProcessorFactory;
import ro.fortsoft.hztask.callback.NotifyMasterTaskFailedOp;
import ro.fortsoft.hztask.callback.NotifyMasterTaskFinishedOp;
import ro.fortsoft.hztask.common.HzKeysConstants;
import ro.fortsoft.hztask.common.task.Task;
import ro.fortsoft.hztask.common.task.TaskKey;
import ro.fortsoft.hztask.util.ClusterUtil;

import java.io.Serializable;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

/**
 *
 * This is the main thread that scans for new Tasks on the queue
 *
 * @author Serban Balamaci
 */
public class TaskConsumerThread extends Thread {

//    private Set<TaskKey> localTaskQueue;

    private BlockingQueue<TaskKey> runningTasks;

    private ClusterAgentServiceImpl clusterAgentService;

    private static final Logger log = LoggerFactory.getLogger(TaskConsumerThread.class);

    private volatile boolean shuttingDown = false;

    public TaskConsumerThread(ClusterAgentServiceImpl clusterAgentService) {
        this.clusterAgentService = clusterAgentService;
        runningTasks = new LinkedBlockingQueue<>(clusterAgentService.getMaxRunningTasks());
    }

    @Override
    public void run() {
        log.info("Started TaskConsumer thread");

        String localClusterId = clusterAgentService.getHzInstance().getCluster().getLocalMember().getUuid();

        IMap<TaskKey, Task> tasksMap = clusterAgentService.getHzInstance().getMap(HzKeysConstants.TASKS_MAP);

        while (true) {
            if(shuttingDown) {
                break;
            }
            try {
                Set<TaskKey> eligibleTasks = tasksMap.keySet(new SqlPredicate("clusterInstanceUuid=" + localClusterId));
                boolean foundTask = false;

                for (TaskKey taskKey : eligibleTasks) {
                    if (!runningTasks.contains(taskKey)) {
                        Task task = tasksMap.get(taskKey);
                        foundTask = true;

                        log.info("Work, work...");
                        startProcessingTask(taskKey, task);
                    }
                }
                if (!foundTask) {
                    log.info("No Tasks found to process, sleeping a while...");
                    Thread.sleep(2000);
                }
            } catch (InterruptedException e) {
                log.info("TaskConsumer Thread received an interrupt signal, stopping");
                break;
            }
        }
    }

    private void startProcessingTask(TaskKey taskKey,Task task) throws InterruptedException {
        runningTasks.put(taskKey);

        TaskProcessorFactory factory = clusterAgentService.getProcessorRegistry().get(task.getClass());
        TaskProcessor taskProcessor = factory.getObject();

        taskProcessor.setTaskKey(taskKey);
        taskProcessor.setTask(task);
        taskProcessor.setClusterAgentService(clusterAgentService);
        taskProcessor.doWork();
    }

    public void notifyTaskFinished(TaskKey taskKey, Serializable result) {
        HazelcastInstance hzInstance = clusterAgentService.getHzInstance();
        IExecutorService executorService = hzInstance.getExecutorService(HzKeysConstants.
                EXECUTOR_SERVICE_FINISHED_TASKS);

        log.info("Notifying Master of task {} finished successfully", taskKey.getTaskId());
        Future masterNotified = executorService.submitToMember(new NotifyMasterTaskFinishedOp(taskKey, result,
                        ClusterUtil.getLocalMemberUuid(hzInstance)),
                clusterAgentService.getMaster());
        try {
            masterNotified.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            log.error("Task {}, failed to notify Master of it's status", e);
        }

        runningTasks.remove(taskKey);
    }

    public void notifyTaskFailed(TaskKey taskKey, Throwable exception) {
        HazelcastInstance hzInstance = clusterAgentService.getHzInstance();
        IExecutorService executorService = hzInstance.getExecutorService(HzKeysConstants.
                EXECUTOR_SERVICE_FINISHED_TASKS);

        Future masterNotified = executorService.submitToMember(new NotifyMasterTaskFailedOp(taskKey, exception,
                ClusterUtil.getLocalMemberUuid(hzInstance)),
                clusterAgentService.getMaster());
        try {
            masterNotified.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            log.error("Task {}, failed to notify Master of it's status", e);
        }

        runningTasks.remove(taskKey);
    }

    public void setShuttingDown(boolean shuttingDown) {
        this.shuttingDown = shuttingDown;
    }
}
