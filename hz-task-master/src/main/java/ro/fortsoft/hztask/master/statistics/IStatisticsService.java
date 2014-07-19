package ro.fortsoft.hztask.master.statistics;

/**
 * @author Serban Balamaci
 */
public interface IStatisticsService {

    public void incTaskFinishedCounter(String taskName, String memberUuid);

    public long getTaskFinishedCountForMember(String memberUuid);

    public void incTaskFailedCounter(String taskName, String memberUuid);

    public long getTaskFailedCountForMember(String memberUuid);

    public void incBacklogTask(String taskName);

    public void decBacklogTask(String taskName);

    public long getBacklogTaskCount(String taskName);

    public long getBacklogTaskCount();

    public void incSubmittedTasks(String taskName, String memberUuid);

    public long getSubmittedTotalTaskCount(String agentUuid);

}
