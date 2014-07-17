package ro.fortsoft.hztask.master.listener;

import com.google.common.eventbus.EventBus;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ro.fortsoft.hztask.master.event.event.AgentLeftEvent;
import ro.fortsoft.hztask.master.event.event.MemberJoinedEvent;

/**
 * @author Serban Balamaci
 */
public class ClusterMembershipListener implements MembershipListener {

    private static final Logger LOG = LoggerFactory.getLogger(ClusterMembershipListener.class);

    private EventBus eventBus;

    public ClusterMembershipListener(EventBus eventBus) {
        this.eventBus = eventBus;
    }

    @Override
    public void memberAdded(MembershipEvent membershipEvent) {
        LOG.info("New member joined {}", membershipEvent.getMember());
        eventBus.post(new MemberJoinedEvent(membershipEvent.getMember()));
    }

    @Override
    public void memberRemoved(MembershipEvent membershipEvent) {
        LOG.info("Member disconnected {}", membershipEvent.getMember());
        eventBus.post(new AgentLeftEvent(membershipEvent.getMember()));
    }

    @Override
    public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {

    }


}
