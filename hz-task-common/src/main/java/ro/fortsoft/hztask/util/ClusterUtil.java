package ro.fortsoft.hztask.util;

import com.google.common.base.Optional;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;

import java.util.Set;

/**
 * @author Serban Balamaci
 */
public class ClusterUtil {

    public static Optional<Member> findMemberWithUuid(HazelcastInstance hzInstance, String memberUuid) {
        Set<Member> members = hzInstance.getCluster().getMembers();
        for(Member member : members) {
            if(member.getUuid().equals(memberUuid)) {
                return Optional.of(member);
            }
        }

        return Optional.absent();
    }

    public static String getLocalMemberUuid(HazelcastInstance hzInstance) {
        return hzInstance.getCluster().getLocalMember().getUuid();
    }

}
