package ro.fortsoft.hztask.agent.executor;

import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ro.fortsoft.hztask.agent.event.task.TaskFailedEvent;
import ro.fortsoft.hztask.agent.event.task.TaskFinishedEvent;
import ro.fortsoft.hztask.agent.processor.TaskProcessor;
import ro.fortsoft.hztask.common.task.Task;
import ro.fortsoft.hztask.common.task.TaskKey;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

/**
 * Service that performs the task execution on the agents
 *
 * @author Serban Balamaci
 */
public class TaskExecutionService {

    private static final Logger log = LoggerFactory.getLogger(TaskExecutionService.class);

    private EventBus eventBus;

    /**
     * Pool for executing the tasks
     */
    private ListeningExecutorService taskExecutorService = MoreExecutors.
            listeningDecorator(Executors.newCachedThreadPool());

    public TaskExecutionService(EventBus eventBus) {
        this.eventBus = eventBus;
    }

    public void executeTask(final TaskProcessor taskProcessor, final TaskKey taskKey, final Task task) {

        ListenableFuture resultFuture = taskExecutorService.submit(new Callable() {
            @Override
            public Object call() throws Exception {
                return taskProcessor.process(task);
            }
        });

        Futures.addCallback(resultFuture, new FutureCallback() {
            @Override
            public void onSuccess(Object result) {
                log.info("SUCCESS FINISH processing for task {}", task);
                eventBus.post(new TaskFinishedEvent(taskKey, task, (Serializable) result));
            }

            @Override
            public void onFailure(Throwable throwable) {
                log.info("FAIL FINISH processing for task {}", task.getId());
                eventBus.post(new TaskFailedEvent(taskKey, throwable));
            }
        });
    }



}
