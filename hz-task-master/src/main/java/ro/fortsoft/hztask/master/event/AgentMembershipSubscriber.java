package ro.fortsoft.hztask.master.event;

import com.google.common.eventbus.Subscribe;
import com.hazelcast.core.Member;
import ro.fortsoft.hztask.master.HazelcastTopologyService;
import ro.fortsoft.hztask.master.distribution.ClusterDistributionService;
import ro.fortsoft.hztask.master.event.event.AgentJoinedEvent;
import ro.fortsoft.hztask.master.event.event.AgentLeftEvent;
import ro.fortsoft.hztask.master.event.event.MemberJoinedEvent;
import ro.fortsoft.hztask.master.scheduler.AgentLeftTaskRescheduler;

/**
 * @author Serban Balamaci
 */
public class AgentMembershipSubscriber {

    private ClusterDistributionService clusterDistributionService;
    private HazelcastTopologyService hazelcastTopologyService;


    @Subscribe
    public void memberJoined(MemberJoinedEvent event) {
        hazelcastTopologyService.callbackWhenAgentReady(event.getMember(), 0);
    }

    @Subscribe
    public void agentJoined(AgentJoinedEvent event) {
        hazelcastTopologyService.addAgent(event.getMember());
    }

    @Subscribe
    public void agentLeft(AgentLeftEvent event) {
        Member agent = event.getMember();
        hazelcastTopologyService.removeAgent(agent);
        AgentLeftTaskRescheduler agentLeftTaskRescheduler =
                new AgentLeftTaskRescheduler(agent, clusterDistributionService);
        agentLeftTaskRescheduler.start();
    }

    public void setClusterDistributionService(ClusterDistributionService clusterDistributionService) {
        this.clusterDistributionService = clusterDistributionService;
    }

    public void setHazelcastTopologyService(HazelcastTopologyService hazelcastTopologyService) {
        this.hazelcastTopologyService = hazelcastTopologyService;
    }
}
