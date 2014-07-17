package ro.fortsoft.hztask.master.event.membership;

import com.hazelcast.core.Member;

/**
 * @author Serban Balamaci
 */
public class MemberJoinedEvent {

    private final Member member;

    public MemberJoinedEvent(Member member) {
        this.member = member;
    }

    public Member getMember() {
        return member;
    }
}
