package ro.fortsoft.hztask.master.service;

import com.google.common.base.Optional;
import ro.fortsoft.hztask.common.task.Task;
import ro.fortsoft.hztask.master.MasterConfig;
import ro.fortsoft.hztask.master.handler.TaskCompletionHandler;
import ro.fortsoft.hztask.master.handler.TaskCompletionHandlerFactory;
import ro.fortsoft.hztask.master.util.NamesUtil;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * TaskCompletionHandlerService that retrieves the {@link TaskCompletionHandler}
 *
 * @author Serban Balamaci
 */
public class TaskCompletionHandlerProvider {

    private MasterConfig masterConfig;

    /**
     * Task executor service that executes the handling of the task result processing
     * so that we don't block any future completed task processing we process them in a separate thread
     */
    private ExecutorService taskExecutorService = Executors.newCachedThreadPool();


    public TaskCompletionHandlerProvider(MasterConfig masterConfig) {
        this.masterConfig = masterConfig;
    }

    /**
     * Method that gets called when the Master is notified of as successfully completed Task
     * @param task Task
     * @param taskResult taskResult
     */
    public void onSuccess(final Task task, final Object taskResult) {
        final String agentName = NamesUtil.toLogFormat(task.getClusterInstanceUuid());

        final Optional<TaskCompletionHandler> finishedTaskHandler = getCompletionHandlerForTask(task);
        if (finishedTaskHandler.isPresent()) {
            taskExecutorService.submit(new Runnable() {
                @Override
                public void run() {
                    finishedTaskHandler.get().onSuccess(task, taskResult, agentName);
                }
            });
        }
    }

    /**
     * Method that gets called when the Master is notified of as completed Task with fail status
     * @param task Task
     * @param exception exception
     */
    public void onFail(final Task task, final Throwable exception) {
        final String agentName = NamesUtil.toLogFormat(task.getClusterInstanceUuid());
        final Optional<TaskCompletionHandler> finishedTaskHandler = getCompletionHandlerForTask(task);
        if (finishedTaskHandler.isPresent()) {
            taskExecutorService.submit(new Runnable() {
                @Override
                public void run() {
                    finishedTaskHandler.get().onFail(task, exception, agentName);
                }
            });
        }
    }

    private Optional<TaskCompletionHandler> getCompletionHandlerForTask(Task task) {
        TaskCompletionHandlerFactory taskCompletionHandlerFactory = masterConfig.
                getFinishedTaskListeners().get(task.getClass());

        if (taskCompletionHandlerFactory != null) {
            TaskCompletionHandler taskCompletionHandler = taskCompletionHandlerFactory.getObject();
            return Optional.fromNullable(taskCompletionHandler);
        }

        if(masterConfig.getDefaultHandlerFactory() != null) {
            TaskCompletionHandler taskCompletionHandler = masterConfig.getDefaultHandlerFactory().getObject();
            return Optional.fromNullable(taskCompletionHandler);
        }
        return Optional.absent();
    }

}
