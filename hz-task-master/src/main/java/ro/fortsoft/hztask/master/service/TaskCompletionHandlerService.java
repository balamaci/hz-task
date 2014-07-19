package ro.fortsoft.hztask.master.service;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import ro.fortsoft.hztask.common.task.Task;
import ro.fortsoft.hztask.master.MasterConfig;
import ro.fortsoft.hztask.master.listener.TaskCompletionListener;
import ro.fortsoft.hztask.master.listener.TaskCompletionListenerFactory;

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
        final Optional<TaskCompletionListener> finishedTaskProcessor = getProcessorForTaskClass(task);
        if (finishedTaskProcessor.isPresent()) {
            taskExecutorService.submit(new Runnable() {
                @Override
                public void run() {
                    finishedTaskProcessor.get().onSuccess(task, taskResult);
                }
            });
        }
    }

    public void onFail(final Task task, final Throwable exception) {
        final Optional<TaskCompletionListener> finishedTaskProcessor = getProcessorForTaskClass(task);
        if (finishedTaskProcessor.isPresent()) {
            taskExecutorService.submit(new Runnable() {
                @Override
                public void run() {
                    finishedTaskProcessor.get().onFail(task, exception);
                }
            });
        }
    }

    private Optional<TaskCompletionListener> getProcessorForTaskClass(Task task) {
        TaskCompletionListenerFactory taskCompletionListenerFactory = masterConfig.
                getFinishedTaskListeners().get(task.getClass());

        if (taskCompletionListenerFactory != null) {
            TaskCompletionListener taskCompletionListener = taskCompletionListenerFactory.getObject();
            return Optional.fromNullable(taskCompletionListener);
        }

        return Optional.absent();
    }

}
