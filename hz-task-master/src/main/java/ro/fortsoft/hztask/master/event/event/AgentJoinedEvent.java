package ro.fortsoft.hztask.master.event.event;

import com.hazelcast.core.Member;

/**
 * @author Serban Balamaci
 */
public class AgentJoinedEvent {

    private final Member member;

    public AgentJoinedEvent(Member member) {
        this.member = member;
    }

    public Member getMember() {
        return member;
    }
}
