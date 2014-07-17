package ro.fortsoft.hztask.master.event.membership;

import com.hazelcast.core.Member;

/**
 * @author Serban Balamaci
 */
public class AgentLeftEvent {

    private final Member member;

    public AgentLeftEvent(Member member) {
        this.member = member;
    }

    public Member getMember() {
        return member;
    }
}
