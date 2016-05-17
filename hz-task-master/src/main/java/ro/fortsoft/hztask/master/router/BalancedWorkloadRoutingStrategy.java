package ro.fortsoft.hztask.master.router;

import com.hazelcast.core.Member;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ro.fortsoft.hztask.common.task.Task;
import ro.fortsoft.hztask.master.statistics.IStatisticsService;
import ro.fortsoft.hztask.master.topology.HazelcastTopologyService;
import ro.fortsoft.hztask.master.util.NamesUtil;

import java.util.Optional;

/**
 * Takes into account the remaining work on agents and their failure ratio
 * If remaining work is small, member should be chosen, but we also need to take into consideration
 * failure ratio
 *
 * So we chose the one with the lowest loadFactor = workload * 1 / failureRatio
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
        float lowestWorkload = Integer.MAX_VALUE;
        Optional<Member> memberToRunOn = Optional.empty();

        for(Member member : hazelcastTopologyService.getAgentsCopy()) {
            String memberUuid = member.getUuid();
            long tasksOfSameTypeSubmitted = statisticsService.
                    getSubmittedTasks(task.getTaskType(), memberUuid);
            long totalTasksSubmitted = statisticsService.getSubmittedTasks(memberUuid);

            long totalProcessedOnMember = statisticsService.getFinishedTasks(memberUuid)
                    + statisticsService.getFailedTasks(memberUuid);
            if(tasksOfSameTypeSubmitted == 0 || totalTasksSubmitted == 0) { //has no work, just joined
                return Optional.of(member);//let's give him some work
            }

            long remainingWorkloadTotal = totalTasksSubmitted - totalProcessedOnMember;
            long tasksOfSameTypeFailed = statisticsService.getFailedTasks(task.getTaskType(), memberUuid);

            //failureFactor : 0.01% is good, 1 means 100% failed
            float failureFactorForTaskType = (float) tasksOfSameTypeFailed / tasksOfSameTypeSubmitted;


            //if remainingWork is small => member chosen so we need to use 1 / failurefactor
            if(remainingWorkloadTotal == 0) {
                remainingWorkloadTotal = 1; //
            }

            float agentLoadFactor = remainingWorkloadTotal;
            if(failureFactorForTaskType != 0) {
                agentLoadFactor = remainingWorkloadTotal * (1 / failureFactorForTaskType);
            }
            log.debug("Remaining work {}, failureRate={} for Member {}", remainingWorkloadTotal,
                    String.format("%.2f", failureFactorForTaskType), NamesUtil.toLogFormat(memberUuid));
            //take member with lowest work load
            if(remainingWorkloadTotal < lowestWorkload) {
                lowestWorkload = agentLoadFactor;
                memberToRunOn = Optional.of(member);
            }
        }

        return memberToRunOn;
    }
}
