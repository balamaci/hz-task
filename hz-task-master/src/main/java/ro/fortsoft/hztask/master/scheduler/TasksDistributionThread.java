package ro.fortsoft.hztask.master.scheduler;

import com.hazelcast.core.Member;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ro.fortsoft.hztask.master.service.ClusterDistributionService;
import ro.fortsoft.hztask.master.statistics.IStatisticsService;

import java.math.BigDecimal;
import java.util.Collection;

/**
 * Handles the distribution of unassigned tasks to available agents, the size of the batch is increased if
 * the new throughput has increased from the last.
 *
 * @author Serban Balamaci
 */
public class TasksDistributionThread extends Thread {

    private ClusterDistributionService clusterDistributionService;

    private double lastThroughput = -1;

    private static final int INIT_BATCH_SIZE = 10;
    private static final int MAX_BATCH_SIZE = 100;
    private int batchSize = INIT_BATCH_SIZE;

    private IStatisticsService statisticsService;

    private static final Logger log = LoggerFactory.getLogger(TasksDistributionThread.class);

    public TasksDistributionThread(ClusterDistributionService clusterDistributionService) {
        setName("Task distribution");

        this.clusterDistributionService = clusterDistributionService;
        statisticsService = clusterDistributionService.getStatisticsService();
    }

    @Override
    public void run() {
        Collection<Member> memberSet = clusterDistributionService.getHazelcastTopologyService().getAgentsCopy();

        long totalSubmittedTasks = getTotalSubmittedTasks(memberSet);
        long totalFinishedTasks = getTotalFinishedTasks(memberSet);

        double throughput = computeThroughput(totalSubmittedTasks, totalFinishedTasks);
        log.info("Found THROUGHPUT {} and lastThroughput={}", throughput, lastThroughput);

        recomputeWindowSize(throughput);

        lastThroughput = throughput;

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

            if(shouldRun) {
                if(batchSize > 0) {
                    clusterDistributionService.rescheduleUnassignedTasks(batchSize);
                }
            }
        } catch (Exception e) {
            log.error("Error encountered during task distribution", e);
        }
    }

    private double computeThroughput(long totalSubmittedTasks, long totalFinishedTasks) {
        //ideal totalS
        double throughput = 0;
        if(totalSubmittedTasks > 0) {
            throughput = (double) totalFinishedTasks / (totalSubmittedTasks);
        }
        BigDecimal throughputBD = new BigDecimal(throughput);
        return throughputBD.setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue();
    }

    private void recomputeWindowSize(double throughput) {
        int oldWindowSize = batchSize;
        if(lastThroughput > throughput) {
            decWindowSize();
        } else {
            incWindowSize();
        }
        log.info("NEW WindowSize={} ---- Previous WindowSize={}", batchSize, oldWindowSize);
    }

    private long getTotalSubmittedTasks(Collection<Member> members) {
        long totalSubmittedTasks = 0;
        for(Member member : members) {
            totalSubmittedTasks += statisticsService.getSubmittedTasks(member.getUuid());
        }
        return totalSubmittedTasks;
    }

    private long getTotalFinishedTasks(Collection<Member> members) {
        long totalFinishedTasks = 0;
        for(Member member : members) {
            totalFinishedTasks += statisticsService.getFinishedTasks(member.getUuid());
        }
        return totalFinishedTasks;
    }

    private void incWindowSize() {
        if(batchSize == 0) {
            batchSize = INIT_BATCH_SIZE;
        }

        if(batchSize < MAX_BATCH_SIZE) {
            batchSize = (int) (batchSize * 1.2);
        }
    }

    private void decWindowSize() {
        batchSize =  (int) (batchSize * 0.8);
    }

    public double getLastThroughput() {
        return lastThroughput;
    }

    public void setLastThroughput(double lastThroughput) {
        this.lastThroughput = lastThroughput;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }
}
