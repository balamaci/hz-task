package ro.fortsoft.hztask.master.event.membership;

import com.google.common.eventbus.Subscribe;
import com.hazelcast.core.Member;
import ro.fortsoft.hztask.master.HazelcastTopologyService;
import ro.fortsoft.hztask.master.service.ClusterDistributionService;
import ro.fortsoft.hztask.master.scheduler.AgentLeftTaskRescheduler;

/**
 * @author Serban Balamaci
 */
public class AgentMembershipSubscriber {

    private ClusterDistributionService clusterDistributionService;
    private HazelcastTopologyService hazelcastTopologyService;


    public AgentMembershipSubscriber(ClusterDistributionService clusterDistributionService,
                                     HazelcastTopologyService hazelcastTopologyService) {
        this.clusterDistributionService = clusterDistributionService;
        this.hazelcastTopologyService = hazelcastTopologyService;
    }

    @Subscribe
    public void memberJoined(MemberJoinedEvent event) {
        hazelcastTopologyService.callbackWhenAgentReady(event.getMember(), 0);
    }

    @Subscribe
    public void agentJoined(AgentJoinedEvent event) {
        hazelcastTopologyService.addAgent(event.getMember());
        clusterDistributionService.startTaskDistributionThread();
    }

    @Subscribe
    public void agentLeft(AgentLeftEvent event) {
        Member agent = event.getMember();
        hazelcastTopologyService.removeAgent(agent);
        AgentLeftTaskRescheduler agentLeftTaskRescheduler =
                new AgentLeftTaskRescheduler(agent, clusterDistributionService);
        agentLeftTaskRescheduler.start();
    }

}
