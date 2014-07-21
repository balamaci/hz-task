package ro.fortsoft.hztask.master.scheduler;

import com.hazelcast.core.Member;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ro.fortsoft.hztask.master.service.ClusterDistributionService;

/**
 * @author Serban Balamaci
 */
public class AgentLeftTaskRescheduler extends Thread {

    private ClusterDistributionService clusterDistributionService;
    private Member member;

    private static final Logger log = LoggerFactory.getLogger(UnassignedTasksReschedulerThread.class);

    public AgentLeftTaskRescheduler(Member member, ClusterDistributionService clusterDistributionService) {
        this.clusterDistributionService = clusterDistributionService;
        this.member = member;
    }

    @Override
    public void run() {
        if(clusterDistributionService.isShuttingDown()) {
            return;
        }

        try {
             clusterDistributionService.rescheduleAgentTasks(member.getUuid());
        } catch (Exception e) {
            log.error("Error rescheduling tasks", e);
        }
    }

}
