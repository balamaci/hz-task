package ro.fortsoft.hztask.master.router;

import com.google.common.base.Optional;
import com.hazelcast.core.Member;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ro.fortsoft.hztask.common.task.Task;
import ro.fortsoft.hztask.master.HazelcastTopologyService;
import ro.fortsoft.hztask.master.statistics.IStatisticsService;
import ro.fortsoft.hztask.master.util.NamesUtil;

/**
 *
 *
 * @author Serban Balamaci
 */
public class BalancedWorkloadRoutingStrategy implements RoutingStrategy {

    private HazelcastTopologyService hazelcastTopologyService;

    private IStatisticsService statisticsService;

    private static final Logger log = LoggerFactory.getLogger(BalancedWorkloadRoutingStrategy.class);

    public BalancedWorkloadRoutingStrategy(HazelcastTopologyService hazelcastTopologyService,
                                           IStatisticsService statisticsService) {
        this.hazelcastTopologyService = hazelcastTopologyService;
        this.statisticsService = statisticsService;
    }

    @Override
    public Optional<Member> getMemberToRunOn(Task task) {
        double min = Integer.MAX_VALUE;
        Optional<Member> nextMember = Optional.absent();

        for(Member member : hazelcastTopologyService.getAgentsCopy()) {
            String memberUuid = member.getUuid();
            long tasksOfSameTypeSubmitted = statisticsService.
                    getSubmittedTaskCount(task.getTaskType(), memberUuid);
            long totalTasksSubmitted = statisticsService.getSubmittedTotalTaskCount(memberUuid);

            long totalProcessedOnMember = statisticsService.getTaskFinishedCountForMember(memberUuid)
                    + statisticsService.getTaskFailedCountForMember(memberUuid);

            if(tasksOfSameTypeSubmitted == 0 || totalTasksSubmitted == 0) { //has no work, just joined
                return Optional.of(member);
            }

            long remainingWorkloadTotal = totalTasksSubmitted - totalProcessedOnMember;

            //failureFactor : 0.01% is good, 1 means 100% failed
            double failureFactorForTaskType = (double) statisticsService.
                    getTaskFailedCountForMember(task.getTaskType(), memberUuid) / tasksOfSameTypeSubmitted;

            //if remainingWork is small => member chosen so we need to use 1 / failurefactor
            if(remainingWorkloadTotal == 0) { //next formula would not take into account failure ratio
                remainingWorkloadTotal = 1;
            }
            double remainingWork = (remainingWorkloadTotal) * (1 / failureFactorForTaskType);
            log.info("Remaining work {} for Member {}", String.format("%.2f", remainingWork),
                    NamesUtil.toLogFormat(memberUuid));
            //take member with lowest work load
            if(remainingWork < min) {
                min = remainingWork;
                nextMember = Optional.of(member);
            }
        }

        return nextMember;
    }
}
