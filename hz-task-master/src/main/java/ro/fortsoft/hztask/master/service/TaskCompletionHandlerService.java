package ro.fortsoft.hztask.master.service;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import ro.fortsoft.hztask.common.task.Task;
import ro.fortsoft.hztask.master.MasterConfig;
import ro.fortsoft.hztask.master.listener.TaskCompletionHandler;
import ro.fortsoft.hztask.master.listener.TaskCompletionHandlerFactory;
import ro.fortsoft.hztask.master.util.NamesUtil;

import java.util.concurrent.Executors;

/**
 * @author Serban Balamaci
 */
public class TaskCompletionHandlerService {

    private MasterConfig masterConfig;

    private ListeningExecutorService taskExecutorService = MoreExecutors.
            listeningDecorator(Executors.newCachedThreadPool());


    public TaskCompletionHandlerService(MasterConfig masterConfig) {
        this.masterConfig = masterConfig;
    }

    public void onSuccess(final Task task, final Object taskResult) {
        final String agentName = NamesUtil.toLogFormat(task.getClusterInstanceUuid());

        final Optional<TaskCompletionHandler> finishedTaskProcessor = getProcessorForTaskClass(task);
        if (finishedTaskProcessor.isPresent()) {
            taskExecutorService.submit(new Runnable() {
                @Override
                public void run() {
                    finishedTaskProcessor.get().onSuccess(task, taskResult, agentName);
                }
            });
        }
    }

    public void onFail(final Task task, final Throwable exception) {
        final String agentName = NamesUtil.toLogFormat(task.getClusterInstanceUuid());
        final Optional<TaskCompletionHandler> finishedTaskProcessor = getProcessorForTaskClass(task);
        if (finishedTaskProcessor.isPresent()) {
            taskExecutorService.submit(new Runnable() {
                @Override
                public void run() {
                    finishedTaskProcessor.get().onFail(task, exception, agentName);
                }
            });
        }
    }

    private Optional<TaskCompletionHandler> getProcessorForTaskClass(Task task) {
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
