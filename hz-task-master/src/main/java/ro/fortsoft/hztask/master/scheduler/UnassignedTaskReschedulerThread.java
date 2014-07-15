package ro.fortsoft.hztask.master.scheduler;

import ro.fortsoft.hztask.master.ClusterMasterServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Serban Balamaci
 */
public class UnassignedTaskReschedulerThread extends Thread {

    private ClusterMasterServiceImpl clusterMasterService;

    private long millisBetweenRuns;

    private static final Logger log = LoggerFactory.getLogger(UnassignedTaskReschedulerThread.class);

    public UnassignedTaskReschedulerThread(ClusterMasterServiceImpl clusterMasterService, long millisBetweenRuns) {
        this.clusterMasterService = clusterMasterService;
        this.millisBetweenRuns = millisBetweenRuns;
    }

    @Override
    public void run() {
        while (true) {
            try {
                clusterMasterService.rescheduleUnassignedTasks();
            } catch (Exception e) {
                log.error("Error rescheduling tasks", e);
            }

            try {
                Thread.sleep(millisBetweenRuns);
            } catch (InterruptedException e) {
                break;
            }
        }
    }
}
