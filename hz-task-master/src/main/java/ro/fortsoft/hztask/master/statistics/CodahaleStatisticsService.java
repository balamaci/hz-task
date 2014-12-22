package ro.fortsoft.hztask.master.statistics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
/**
 * @author Serban Balamaci
 */
public class CodahaleStatisticsService implements IStatisticsService {

    private final MetricRegistry metrics = new MetricRegistry();


    @Override
    public void incTaskFinishedCounter(String taskName, String memberUuid) {
        Counter tasks = metrics.counter("finished-tasks-type," + taskName + "," + memberUuid);
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
    public void incTaskFailedCounter(String taskName, String memberUuid) {
        Counter tasksFailed = metrics.counter("failed-tasks-type," + taskName + "," + memberUuid);
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
    public long getTaskFailedCountForMember(String taskName, String memberUuid) {
        Counter tasksFailed = metrics.counter("failed-tasks-type," + taskName + "," + memberUuid);
        return tasksFailed.getCount();
    }

    @Override
    public void incBacklogTask(String taskName) {
        Counter totalBacklog = metrics.counter("backlog-tasks-total");
        Counter tasks = metrics.counter("backlog-tasks," + taskName);
        totalBacklog.inc();
        tasks.inc();
    }

    @Override
    public void decBacklogTask(String taskName) {
        Counter totalBacklog = metrics.counter("backlog-tasks-total");
        Counter tasks = metrics.counter("backlog-tasks," + taskName);

        totalBacklog.dec();
        tasks.dec();
    }

    @Override
    public long getBacklogTaskCount() {
        Counter tasks = metrics.counter("backlog-tasks");
        return tasks.getCount();
    }

    @Override
    public long getBacklogTaskCount(String taskName) {
        Counter tasks = getBacklogTaskCounter(taskName);
        return tasks.getCount();
    }

    private Counter getBacklogTaskCounter(String taskName) {
        return metrics.counter("backlog-tasks," + taskName);
    }

    @Override
    public void incSubmittedTasks(String taskName, String memberUuid) {
        Counter tasks = metrics.counter("agent-tasks-type," + taskName + "," + memberUuid);
        Counter byMember = metrics.counter("agent-tasks-member," + memberUuid);

        tasks.inc();
        byMember.inc();
    }

    @Override
    public long getSubmittedTotalTaskCount(String agentUuid) {
        Counter byMember = metrics.counter("agent-tasks-member," + agentUuid);
        return byMember.getCount();
    }
}
