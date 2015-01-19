package ro.fortsoft.hztask.master.statistics;

/**
 * @author Serban Balamaci
 */
public interface IStatisticsService {

    public void incTaskFinishedCounter(String taskType, String agentUuid);

    public long getFinishedTasks(String taskType, String agentUuid);
    
    public long getFinishedTasks(String agentUuid);

    public void incTaskFailedCounter(String taskType, String agentUuid);

    public long getFailedTasks(String agentUuid);

    public long getFailedTasks(String taskType, String agentUuid);

    public void incSubmittedTasks(String taskType, String agentUuid);

    public long getSubmittedTasks(String agentUuid);

    public long getSubmittedTasks(String taskType, String agentUuid);

    public void incUnassignedTasks(String taskType);

    public void decUnassignedTask(String taskType);

}
