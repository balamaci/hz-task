package ro.fortsoft.hztask.agent.executor;

import com.google.common.eventbus.EventBus;
import javaslang.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ro.fortsoft.hztask.agent.event.task.TaskFailedEvent;
import ro.fortsoft.hztask.agent.event.task.TaskFinishedEvent;
import ro.fortsoft.hztask.agent.processor.TaskProcessor;
import ro.fortsoft.hztask.common.task.Task;
import ro.fortsoft.hztask.common.task.TaskKey;

import java.io.Serializable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Service that performs the task execution on the agents
 *
 * @author Serban Balamaci
 */
public class TaskExecutionService {

    private static final Logger log = LoggerFactory.getLogger(TaskExecutionService.class);

    private EventBus eventBus;

    /** Pool for executing the tasks */
    private ExecutorService taskExecutorService = Executors.newCachedThreadPool();

    public TaskExecutionService(EventBus eventBus) {
        this.eventBus = eventBus;
    }

    public void executeTask(final TaskProcessor taskProcessor, final TaskKey taskKey, final Task task) {
        Future<? extends Serializable> resultFuture = Future.of(taskExecutorService, ()-> taskProcessor.process(task));
        resultFuture
                .onSuccess((result) -> {
                    log.info("SUCCESS FINISH processing for task {}", task);
                    eventBus.post(new TaskFinishedEvent(taskKey, task, result));
                })
                .onFailure((throwable) -> {
                    log.info("FAIL FINISH processing for task {}", task.getId());
                    eventBus.post(new TaskFailedEvent(taskKey, throwable));
                });
    }



}
