package ro.fortsoft.hztask.master.event;

import com.google.common.eventbus.Subscribe;
import ro.fortsoft.hztask.master.event.event.AgentJoinedEvent;

/**
 * @author Serban Balamaci
 */
public class AgentMembershipSubscriber {

    @Subscribe
    public void agentJoined(AgentJoinedEvent event) {

    }

}
