package com.fortsoft.hztask.master.listener;

import com.fortsoft.hztask.master.HazelcastTopologyService;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Serban Balamaci
 */
public class ClusterMembershipListener implements MembershipListener {

    private static final Logger LOG = LoggerFactory.getLogger(ClusterMembershipListener.class);

    private HazelcastTopologyService hazelcastTopologyService;

    public ClusterMembershipListener(HazelcastTopologyService hazelcastTopologyService) {
        this.hazelcastTopologyService = hazelcastTopologyService;
    }

    @Override
    public void memberAdded(MembershipEvent membershipEvent) {
        LOG.info("New member joined {}", membershipEvent.getMember());
        hazelcastTopologyService.callbackWhenAgentReady(membershipEvent.getMember(), 0);
    }

    @Override
    public void memberRemoved(MembershipEvent membershipEvent) {
        LOG.info("Member disconnected {}", membershipEvent.getMember());
        hazelcastTopologyService.removeAgent(membershipEvent.getMember());
    }

    @Override
    public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {

    }


}
