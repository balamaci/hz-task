package ro.fortsoft.hztask.master.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ro.fortsoft.hztask.master.MasterConfig;
import ro.fortsoft.hztask.master.service.ClusterDistributionService;

/**
 * @author Serban Balamaci
 */
public class UnassignedTasksReschedulerThread extends Thread {

    private ClusterDistributionService clusterDistributionService;

    private final long millisBetweenRuns = 5000;

    private static final Logger log = LoggerFactory.getLogger(UnassignedTasksReschedulerThread.class);

    public UnassignedTasksReschedulerThread(ClusterDistributionService clusterDistributionService,
                                            MasterConfig masterConfig) {
        this.clusterDistributionService = clusterDistributionService;
//        this.millisBetweenRuns = masterConfig.getUnassignedTaskReschedulerWaitTimeMs();
    }

    @Override
    public void run() {
        while (true) {
            try {
                boolean shouldRun = true;
                if(clusterDistributionService.getTaskCount() == 0) {
                    log.info("No tasks to redistribute");
                    shouldRun = false;
                }

                if(clusterDistributionService.getAgentsCount() == 0) {
                    log.info("No Agents to redistribute task to");
                    shouldRun = false;
                }

                boolean taskFound = false;
                if(shouldRun) {
                    taskFound = clusterDistributionService.rescheduleUnassignedTasks(100);
                }

                if(! taskFound) {
                    Thread.sleep(millisBetweenRuns);
                }
            } catch (InterruptedException e) {
                log.info("{} received interrupt, terminating", getName());
                break;
            } catch (Exception e) {
                log.error("Error rescheduling tasks", e);
            }
        }
    }
}
