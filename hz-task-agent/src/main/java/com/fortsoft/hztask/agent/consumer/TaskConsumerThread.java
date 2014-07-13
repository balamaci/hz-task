package com.fortsoft.hztask.agent.consumer;

import com.fortsoft.hztask.ClusterAgent;
import com.fortsoft.hztask.TaskProcessor;
import com.fortsoft.hztask.TaskProcessorFactory;
import com.fortsoft.hztask.callback.TaskFailedOp;
import com.fortsoft.hztask.callback.TaskFinishedOp;
import com.fortsoft.hztask.common.task.Task;
import com.fortsoft.hztask.common.task.TaskKey;
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
 * @author Serban Balamaci
 */
public class TaskConsumerThread extends Thread {

//    private Set<TaskKey> localTaskQueue;

    private BlockingQueue<TaskKey> runningTasks;

    private ClusterAgent clusterAgent;


    private static final Logger log = LoggerFactory.getLogger(TaskConsumerThread.class);

    public TaskConsumerThread(ClusterAgent clusterAgent) {
        this.clusterAgent = clusterAgent;
        runningTasks = new LinkedBlockingQueue<>(clusterAgent.getMaxRunningTasks());
    }

    @Override
    public void start() {
        log.info("Started TaskConsumer thread");

        String localClusterId = clusterAgent.getHzInstance().getCluster().getLocalMember().getUuid();

        IMap<TaskKey, Task> tasksMap = clusterAgent.getHzInstance().getMap("tasks");

        while (true) {
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
                    break;
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

        TaskProcessorFactory factory = clusterAgent.getProcessorRegistry().get(task.getClass());
        TaskProcessor taskProcessor = factory.getObject();

        taskProcessor.setTaskKey(taskKey);
        taskProcessor.setTask(task);
        taskProcessor.setClusterAgent(clusterAgent);
        taskProcessor.doWork();
    }

    public void notifyTaskFinished(TaskKey taskKey, Serializable result) {
        IExecutorService executorService = clusterAgent.getHzInstance().getExecutorService("finishedTasks");

        log.info("Notifing Master of task {} finished succesfully", taskKey.getTaskId());
        Future masterNotified = executorService.submitToMember(new TaskFinishedOp(taskKey, result), clusterAgent.getMaster());
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
        IExecutorService executorService = clusterAgent.getHzInstance().getExecutorService("finishedTasks");

        executorService.submitToMember(new TaskFailedOp(taskKey, exception), clusterAgent.getMaster());

        runningTasks.remove(taskKey);
    }

}
