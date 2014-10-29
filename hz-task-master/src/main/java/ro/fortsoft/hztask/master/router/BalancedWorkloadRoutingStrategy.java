package ro.fortsoft.hztask.master.router;

import com.google.common.base.Optional;
import com.hazelcast.core.Member;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ro.fortsoft.hztask.master.HazelcastTopologyService;
import ro.fortsoft.hztask.master.statistics.IStatisticsService;
import ro.fortsoft.hztask.master.util.NamesUtil;

/**
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
    public Optional<Member> getMemberToRunOn() {
        double min = Integer.MAX_VALUE;
        Optional<Member> nextMember = Optional.absent();

        for(Member member : hazelcastTopologyService.getAgentsCopy()) {
            String memberUuid = member.getUuid();
            long totalSubmitted = statisticsService.getSubmittedTotalTaskCount(memberUuid);
            long totalProcessed = statisticsService.getTaskFinishedCountForMember(memberUuid)
                    + statisticsService.getTaskFailedCountForMember(memberUuid);

            if(totalSubmitted == 0) {
                return Optional.of(member);
            }

            long remainingWorkload = totalSubmitted - totalProcessed;
            double failureFactor = (double) statisticsService.
                    getTaskFailedCountForMember(memberUuid) / totalSubmitted; // 0.01% is good, 1 means 100% failed

            double actualRemainingWork = remainingWorkload + remainingWorkload * failureFactor;
            log.info("Remaining work {} for Member {}", String.format("%.2f", actualRemainingWork),
                    NamesUtil.toLogFormat(memberUuid));
            //the biggest the failure factor the most
            if(actualRemainingWork < min) {
                min = actualRemainingWork;
                nextMember = Optional.of(member);
            }
        }

        return nextMember;
    }
}
