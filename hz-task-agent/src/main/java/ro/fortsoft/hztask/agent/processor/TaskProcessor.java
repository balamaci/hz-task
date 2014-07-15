package ro.fortsoft.hztask.agent.processor;

import ro.fortsoft.hztask.agent.ClusterAgentServiceImpl;
import ro.fortsoft.hztask.common.task.Task;
import ro.fortsoft.hztask.common.task.TaskKey;
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
 * Override this class and inject from the Factory the dependencies
 *
 * @author Serban Balamaci
 */
public abstract class TaskProcessor<T extends Serializable> {

    private Task task;
    private TaskKey taskKey;

    private ClusterAgentServiceImpl clusterAgentService;

    private static final Logger log = LoggerFactory.getLogger(TaskProcessor.class);

    /**
     * Internal method that handles the processing the task
     * @return
     */
    public final ListenableFuture<T> doWork() {
        ListenableFuture<T> resultFuture = clusterAgentService.getTaskExecutorService().submit(new Callable<T>() {

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

    /**
     * Method to override and implement what the task is supposed to do
     * @param task Task that holds the data
     * @return processing result
     */
    public abstract T process(Task task);

    private void onComplete(T result) {
        clusterAgentService.getTaskConsumerThread().notifyTaskFinished(taskKey, result);
    }

    private void onFail(Throwable throwable) {
        clusterAgentService.getTaskConsumerThread().notifyTaskFailed(taskKey, throwable);
    }

    public void setTaskKey(TaskKey taskKey) {
        this.taskKey = taskKey;
    }

    public void setTask(Task task) {
        this.task = task;
    }

    public void setClusterAgentService(ClusterAgentServiceImpl clusterAgentService) {
        this.clusterAgentService = clusterAgentService;
    }
}
