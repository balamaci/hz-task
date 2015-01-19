package ro.fortsoft.hztask.master.statistics;

/**
 * @author Serban Balamaci
 */
public interface IStatisticsService {

    public void incTaskFinishedCounter(String taskType, String memberUuid);

    public long getTaskFinishedCountForMember(String memberUuid);

    public void incTaskFailedCounter(String taskType, String memberUuid);

    public long getTaskFailedCountForMember(String memberUuid);

    public long getTaskFailedCountForMember(String taskType, String memberUuid);

    public void incBacklogTask(String taskType);

    public void decBacklogTask(String taskType);

    public long getBacklogTaskCount(String taskType);

    public long getBacklogTaskCount();

    public void incSubmittedTasks(String taskType, String memberUuid);

    public long getSubmittedTotalTaskCount(String memberUuid);

    public long getSubmittedTaskCount(String taskType, String memberUuid);

}
