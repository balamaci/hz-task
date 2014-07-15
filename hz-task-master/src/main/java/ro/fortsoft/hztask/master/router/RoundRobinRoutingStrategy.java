package ro.fortsoft.hztask.master.router;

import ro.fortsoft.hztask.master.HazelcastTopologyService;
import com.google.common.base.Optional;
import com.hazelcast.core.Member;

/**
 * @author Serban Balamaci
 */
public class RoundRobinRoutingStrategy implements RoutingStrategy {

    private HazelcastTopologyService hazelcastTopologyService;

    private int roundRobinCounter = 0;

    public RoundRobinRoutingStrategy(HazelcastTopologyService hazelcastTopologyService) {
        this.hazelcastTopologyService = hazelcastTopologyService;
    }

    @Override
    public Optional<Member> getMemberToRunOn() {
        if(hazelcastTopologyService.getAgents().size() == 0) {
            return Optional.absent();
        }

        roundRobinCounter ++;
        roundRobinCounter = roundRobinCounter % hazelcastTopologyService.getAgents().size();

        return Optional.of(hazelcastTopologyService.getAgents().get(roundRobinCounter));
    }
}
