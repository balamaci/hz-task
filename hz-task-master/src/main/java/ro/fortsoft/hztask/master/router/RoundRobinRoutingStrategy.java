package ro.fortsoft.hztask.master.router;

import com.google.common.base.Optional;
import com.hazelcast.core.Member;
import ro.fortsoft.hztask.common.task.Task;
import ro.fortsoft.hztask.master.topology.HazelcastTopologyService;

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
    public synchronized Optional<Member> getMemberToRunOn(Task task) {
        int numberOfAgents = hazelcastTopologyService.getAgentsCount();
        if(numberOfAgents == 0) {
            return Optional.absent();
        }

        roundRobinCounter ++;
        roundRobinCounter = roundRobinCounter % numberOfAgents;

        return Optional.of(hazelcastTopologyService.getAgentsCopy().get(roundRobinCounter));
    }
}
