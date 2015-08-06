package ro.fortsoft.hztask.agent.event.task;

import com.google.common.eventbus.Subscribe;
import ro.fortsoft.hztask.agent.ClusterAgentService;
import ro.fortsoft.hztask.agent.finalizer.TaskFinishedHandler;

/**
 * Subscriber that reacts to the {@link TaskFinishedEvent} and {@link TaskFailedEvent} events
 *
 * @author Serban Balamaci
 */
public class TaskEventSubscriber {

    private TaskFinishedHandler taskFinishedHandler;

    public TaskEventSubscriber(ClusterAgentService clusterAgentService) {
        this.taskFinishedHandler = new TaskFinishedHandler(clusterAgentService);
    }

    @Subscribe
    public void onTaskFinishedEvent(TaskFinishedEvent ev) {
        taskFinishedHandler.success(ev.getTaskKey(), ev.getResult());
    }

    @Subscribe
    public void onTaskFailedEvent(TaskFailedEvent ev) {
        taskFinishedHandler.failure(ev.getTaskKey(), ev.getException());
    }

}
