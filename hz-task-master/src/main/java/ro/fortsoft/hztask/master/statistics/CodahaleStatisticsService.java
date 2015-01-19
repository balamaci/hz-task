package ro.fortsoft.hztask.master.statistics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
/**
 * @author Serban Balamaci
 */
public class CodahaleStatisticsService implements IStatisticsService {

    private final MetricRegistry metrics = new MetricRegistry();


    @Override
    public void incTaskFinishedCounter(String taskType, String memberUuid) {
        Counter tasks = metrics.counter("finished-tasks-type," + taskType + "," + memberUuid);
        Counter byMember = metrics.counter("finished-tasks-member," + memberUuid);

        tasks.inc();
        byMember.inc();
    }

    @Override
    public long getTaskFinishedCountForMember(String memberUuid) {
        Counter byMember = metrics.counter("finished-tasks-member," + memberUuid);
        return byMember.getCount();
    }

    @Override
    public void incTaskFailedCounter(String taskType, String memberUuid) {
        Counter tasksFailed = metrics.counter("failed-tasks-type," + taskType + "," + memberUuid);
        Counter byMember = metrics.counter("failed-tasks-member," + memberUuid);

        tasksFailed.inc();
        byMember.inc();
    }

    @Override
    public long getTaskFailedCountForMember(String memberUuid) {
        Counter byMember = metrics.counter("failed-tasks-member," + memberUuid);
        return byMember.getCount();
    }

    @Override
    public long getTaskFailedCountForMember(String taskType, String memberUuid) {
        Counter tasksFailed = metrics.counter("failed-tasks-type," + taskType + "," + memberUuid);
        return tasksFailed.getCount();
    }

    @Override
    public void incBacklogTask(String taskType) {
        Counter totalBacklog = metrics.counter("backlog-tasks-total");
        Counter tasks = metrics.counter("backlog-tasks," + taskType);
        totalBacklog.inc();
        tasks.inc();
    }

    @Override
    public void decBacklogTask(String taskType) {
        Counter totalBacklog = metrics.counter("backlog-tasks-total");
        Counter tasks = metrics.counter("backlog-tasks," + taskType);

        totalBacklog.dec();
        tasks.dec();
    }

    @Override
    public long getBacklogTaskCount() {
        Counter tasks = metrics.counter("backlog-tasks");
        return tasks.getCount();
    }

    @Override
    public long getBacklogTaskCount(String taskType) {
        Counter tasks = getBacklogTaskCounter(taskType);
        return tasks.getCount();
    }

    private Counter getBacklogTaskCounter(String taskType) {
        return metrics.counter("backlog-tasks," + taskType);
    }

    @Override
    public void incSubmittedTasks(String taskType, String memberUuid) {
        Counter tasks = metrics.counter("agent-tasks-type," + taskType + "," + memberUuid);
        Counter byMember = metrics.counter("agent-tasks-member," + memberUuid);

        tasks.inc();
        byMember.inc();
    }

    @Override
    public long getSubmittedTotalTaskCount(String memberUuid) {
        Counter byMember = metrics.counter("agent-tasks-member," + memberUuid);
        return byMember.getCount();
    }

    @Override
    public long getSubmittedTaskCount(String taskType, String agentUuid) {
        Counter tasks = metrics.counter("agent-tasks-type," + taskType + "," + agentUuid);
        Counter byMember = metrics.counter("agent-tasks-member," + agentUuid);
        return byMember.getCount();
    }

}
