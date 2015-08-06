package ro.fortsoft.hztask.agent.event.task;

import com.google.common.eventbus.Subscribe;
import ro.fortsoft.hztask.agent.ClusterAgentService;
import ro.fortsoft.hztask.agent.finalizer.TaskFinalizer;

/**
 * Subscriber that reacts to the {@link TaskFinishedEvent} and {@link TaskFailedEvent} events
 *
 * @author Serban Balamaci
 */
public class TaskEventSubscriber {

    private ClusterAgentService clusterAgentService;

    public TaskEventSubscriber(ClusterAgentService clusterAgentService) {
        this.clusterAgentService = clusterAgentService;
    }

    @Subscribe
    public void onTaskFinishedEvent(TaskFinishedEvent ev) {
        new TaskFinalizer(clusterAgentService).notifyTaskFinished(ev.getTaskKey(),
                ev.getResult());
    }

    @Subscribe
    public void onTaskFailedEvent(TaskFailedEvent ev) {
        new TaskFinalizer(clusterAgentService).notifyTaskFailed(ev.getTaskKey(),
                ev.getException());
    }

}
