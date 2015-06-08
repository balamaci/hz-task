package ro.fortsoft.hztask.agent.event.task;

import com.google.common.eventbus.Subscribe;
import ro.fortsoft.hztask.agent.ClusterAgentService;

/**
 * Subscriber that reacts to the TaskFinished and TaskFailed events
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
        clusterAgentService.getTaskConsumerThread().notifyTaskFinished(ev.getTaskKey(),
                ev.getResult());
    }

    @Subscribe
    public void onTaskFailedEvent(TaskFailedEvent ev) {
        clusterAgentService.getTaskConsumerThread().notifyTaskFailed(ev.getTaskKey(),
                ev.getException());
    }

}
