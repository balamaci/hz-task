package ro.fortsoft.hztask.master.statistics.codahale;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import ro.fortsoft.hztask.master.statistics.IStatisticsService;

/**
 * Implementation of StatisticsService with Codahale metrics
 *
 * @author Serban Balamaci
 */
public class CodahaleStatisticsService implements IStatisticsService {

    private final MetricRegistry metrics = new MetricRegistry();


    @Override
    public void incTaskFinishedCounter(String taskType, String agentUuid) {
        Counter tasks = finishedTasksCounter(agentUuid);
        Counter byMember = finishedTasksCounter(taskType, agentUuid);

        tasks.inc();
        byMember.inc();
    }

    @Override
    public long getFinishedTasks(String taskType, String agentUuid) {
        return finishedTasksCounter(taskType, agentUuid).getCount();
    }

    @Override
    public long getFinishedTasks(String agentUuid) {
        return finishedTasksCounter(agentUuid).getCount();
    }

    @Override
    public void incTaskFailedCounter(String taskType, String agentUuid) {
        Counter tasksFailed = failedTasksCounter(taskType, agentUuid);
        Counter byMember = failedTasksCounter(taskType, agentUuid);

        tasksFailed.inc();
        byMember.inc();
    }

    @Override
    public long getFailedTasks(String agentUuid) {
        return failedTasksCounter(agentUuid).getCount();
    }

    @Override
    public long getFailedTasks(String taskType, String agentUuid) {
        return failedTasksCounter(taskType, agentUuid).getCount();
    }

    @Override
    public void incSubmittedTasks(String taskType, String agentUuid) {
        Counter tasksType = submittedTasksCounter(taskType, agentUuid);
        Counter byMember = submittedTasksCounter(agentUuid);

        tasksType.inc();
        byMember.inc();
    }

    @Override
    public long getSubmittedTasks(String agentUuid) {
        return submittedTasksCounter(agentUuid).getCount();
    }

    @Override
    public long getSubmittedTasks(String taskType, String agentUuid) {
        return submittedTasksCounter(taskType, agentUuid).getCount();
    }

    @Override
    public void incUnassignedTasks(String taskType) {
        unassignedTasksCounter(taskType).inc();
    }

    @Override
    public void decUnassignedTask(String taskType) {
        unassignedTasksCounter(taskType).dec();
    }

    private Counter unassignedTasksCounter(String taskType) {
        return metrics.counter("unassigned-tasks," + taskType);
    }

    private Counter failedTasksCounter(String taskType, String agentUuid) {
        return metrics.counter("failed-tasks-type-agent," + taskType + "," + agentUuid);
    }

    private Counter failedTasksCounter(String agentUuid) {
        return metrics.counter("failed-tasks-agent," + agentUuid);
    }

    private Counter finishedTasksCounter(String taskType, String agentUuid) {
        return metrics.counter("finished-tasks-type-agent," + taskType + "," + agentUuid);
    }

    private Counter finishedTasksCounter(String agentUuid) {
        return metrics.counter("finished-tasks-agent," + agentUuid);
    }

    private Counter submittedTasksCounter(String taskType, String agentUuid) {
        return metrics.counter("submitted-tasks-type-agent," + taskType + "," + agentUuid);
    }

    private Counter submittedTasksCounter(String agentUuid) {
        return metrics.counter("submitted-tasks-agent," + agentUuid);
    }
}
