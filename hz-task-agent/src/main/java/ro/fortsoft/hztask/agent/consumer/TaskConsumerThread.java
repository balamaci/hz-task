package ro.fortsoft.hztask.agent.consumer;

import ro.fortsoft.hztask.agent.ClusterAgentServiceImpl;
import ro.fortsoft.hztask.agent.processor.TaskProcessor;
import ro.fortsoft.hztask.agent.processor.TaskProcessorFactory;
import ro.fortsoft.hztask.callback.TaskFailedOp;
import ro.fortsoft.hztask.callback.TaskFinishedOp;
import ro.fortsoft.hztask.common.HzKeysConstants;
import ro.fortsoft.hztask.common.task.Task;
import ro.fortsoft.hztask.common.task.TaskKey;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.IMap;
import com.hazelcast.query.SqlPredicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    //nothing, probably shuttingdown
                }
            }
        }
    }

    private void startProcessingTask(TaskKey taskKey,Task task) {
        try {
            runningTasks.put(taskKey);
        } catch (InterruptedException e) {
            return;
        }

        TaskProcessorFactory factory = clusterAgentService.getProcessorRegistry().get(task.getClass());
        TaskProcessor taskProcessor = factory.getObject();

        taskProcessor.setTaskKey(taskKey);
        taskProcessor.setTask(task);
        taskProcessor.setClusterAgentService(clusterAgentService);
        taskProcessor.doWork();
    }

    public void notifyTaskFinished(TaskKey taskKey, Serializable result) {
        IExecutorService executorService = clusterAgentService.getHzInstance().
                getExecutorService(HzKeysConstants.EXECUTOR_SERVICE_FINISHED_TASKS);

        log.info("Notifing Master of task {} finished succesfully", taskKey.getTaskId());
        Future masterNotified = executorService.submitToMember(new TaskFinishedOp(taskKey, result),
                clusterAgentService.getMaster());
        try {
            masterNotified.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        runningTasks.remove(taskKey);
    }

    public void notifyTaskFailed(TaskKey taskKey, Throwable exception) {
        IExecutorService executorService = clusterAgentService.getHzInstance().
                getExecutorService(HzKeysConstants.EXECUTOR_SERVICE_FINISHED_TASKS);

        executorService.submitToMember(new TaskFailedOp(taskKey, exception), clusterAgentService.getMaster());

        runningTasks.remove(taskKey);
    }

    public void setShuttingDown(boolean shuttingDown) {
        this.shuttingDown = shuttingDown;
    }
}
