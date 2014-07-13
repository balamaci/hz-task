package com.fortsoft.hztask;

import com.fortsoft.hztask.common.task.Task;
import com.fortsoft.hztask.common.task.TaskKey;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.concurrent.Callable;

/**
 * Base class for processors of task to implement
 *
 * @author Serban Balamaci
 */
public abstract class TaskProcessor<T extends Serializable> implements Serializable {

    private Task task;
    private TaskKey taskKey;

    private ClusterAgent clusterAgent;

    private static final Logger log = LoggerFactory.getLogger(TaskProcessor.class);



    public final ListenableFuture<T> doWork() {
        ListenableFuture<T> resultFuture = clusterAgent.getTaskExecutorService().submit(new Callable<T>() {

            @Override
            public T call() throws Exception {
                return process(task);
            }
        });

        Futures.addCallback(resultFuture, new FutureCallback<T>() {
            @Override
            public void onSuccess(T result) {
                log.info("Finished task {}", task.getId());
                onComplete(result);
            }

            @Override
            public void onFailure(Throwable throwable) {
                log.info("Received error for task {}", task.getId());
                onFail(throwable);
            }
        });

        return resultFuture;
    }

    public abstract T process(Object task);

    private void onComplete(T result) {
        clusterAgent.getTaskConsumerThread().notifyTaskFinished(taskKey, result);
    }

    private void onFail(Throwable throwable) {
        clusterAgent.getTaskConsumerThread().notifyTaskFailed(taskKey, throwable);
    }

    public void setTaskKey(TaskKey taskKey) {
        this.taskKey = taskKey;
    }

    public void setTask(Task task) {
        this.task = task;
    }

    public void setClusterAgent(ClusterAgent clusterAgent) {
        this.clusterAgent = clusterAgent;
    }
}
