package ro.fortsoft.hztask.agent.listener;

import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import ro.fortsoft.hztask.agent.ClusterAgentService;

/**
 * @author Serban Balamaci
 */
public class ClusterMembershipListener implements MembershipListener {

    private ClusterAgentService clusterAgentService;

    public ClusterMembershipListener(ClusterAgentService clusterAgentService) {
        this.clusterAgentService = clusterAgentService;
    }

    @Override
    public void memberAdded(MembershipEvent membershipEvent) {
    }

    @Override
    public void memberRemoved(MembershipEvent membershipEvent) {
        if(membershipEvent.getMember().equals(clusterAgentService.getMaster())) {
            clusterAgentService.handleMasterLeft();
        }
    }

    @Override
    public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {

    }
}
