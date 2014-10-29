package ro.fortsoft.hztask.master;

import com.google.common.collect.Lists;
import com.google.common.eventbus.AsyncEventBus;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ro.fortsoft.hztask.common.HzKeysConstants;
import ro.fortsoft.hztask.common.MemberType;
import ro.fortsoft.hztask.master.event.membership.AgentJoinedEvent;
import ro.fortsoft.hztask.master.service.CommunicationService;
import ro.fortsoft.hztask.master.util.NamesUtil;
import ro.fortsoft.hztask.op.GetMemberTypeClusterOp;
import ro.fortsoft.hztask.op.agent.AnnounceMasterAndSignalStartWorkOp;
import ro.fortsoft.hztask.op.agent.AskAgentReadyOp;

import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Class that keeps evidence of cluster membership
 *
 * @author Serban Balamaci
 */
public class HazelcastTopologyService {

    private final CommunicationService communicationService;

    private final CopyOnWriteArraySet<Member> agents = new CopyOnWriteArraySet<>();

    private final HazelcastInstance hzInstance;

    private static final int MAX_ASK_READY_ATTEMPTS = 5;

    private final AsyncEventBus eventBus;

    private static final Logger log = LoggerFactory.getLogger(HazelcastTopologyService.class);

    public HazelcastTopologyService(HazelcastInstance hzInstance, AsyncEventBus eventBus,
                                    CommunicationService communicationService) {
        this.hzInstance = hzInstance;
        this.eventBus = eventBus;
        this.communicationService = communicationService;
    }

    private Future<MemberType> isMemberMaster(Member member) {
        return communicationService.sendMessageToMember(member, new GetMemberTypeClusterOp());
    }

    public boolean isMasterAmongClusterMembers() {
        Set<Member> members = hzInstance.getCluster().getMembers();
        List<Future<MemberType>> futures = Lists.newArrayList();

        for(Member member : members) {
            if(! member.localMember()) {
                futures.add(isMemberMaster(member));
            }
        }

        for(Future<MemberType> future : futures) {
            try {
                MemberType memberType = future.get();
                if(MemberType.MASTER.equals(memberType)) {
                    return true;
                }
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        return false;
    }

    public void callbackWhenAgentReady(Member member, int attempt) {
        if(attempt > MAX_ASK_READY_ATTEMPTS) {
            log.error("NON_JOIN_READY_AFTER_MANY_ATTEMPTS: After {} attempts the member {} " +
                    "did not respond affirmatively", attempt);
            return;
        }

        try {
            log.info("Asking {} if READY attempt {}", member, attempt);
            communicationService.submitToMember(member, new AskAgentReadyOp(),
                     new MemberReadyCallback(member, attempt));
        } catch (Exception e) {
            log.error("Error sending AskAgentReadyOp", e);
        }
    }



    public void addAgent(Member member) {
        agents.add(member);
        String name = member.getStringAttribute(HzKeysConstants.AGENT_NAME_PROPERTY);
        if(name != null) {
            NamesUtil.addMember(member.getUuid(), name);
        }
    }

    public void removeAgent(Member member) {
        agents.remove(member);
    }

    public List<Member> getAgentsCopy() {
        return Lists.newArrayList(agents);
    }

    public int getAgentsCount() {
        return agents.size();
    }

    public Member getMaster() {
        return hzInstance.getCluster().getLocalMember();
    }


    private class MemberReadyCallback implements ExecutionCallback<Boolean> {

        private Member member;
        private int attempt;

        private MemberReadyCallback(Member member, int attempt) {
            this.member = member;
            this.attempt = attempt;
        }

        @Override
        public void onResponse(Boolean response) {
            if(response) {
                log.info("NEW_JOIN New cluster agent {} ID={} is active", member, member.getUuid());
                communicationService.sendMessageToMember(member, new AnnounceMasterAndSignalStartWorkOp(getMaster()));

                eventBus.post(new AgentJoinedEvent(member));
            } else {
                TimerTask timerTask = new TimerTask() {
                    @Override
                    public void run() {
                        callbackWhenAgentReady(member, ++ attempt);
                    }
                };
                new Timer().schedule(timerTask, 5000);
            }
        }

        @Override
        public void onFailure(Throwable t) {
            log.error("EXCEPTION_ON_JOIN Agent responded with failure", t);
        }
    }

    public Member getLocalMember() {
        return hzInstance.getCluster().getLocalMember();
    }

    public HazelcastInstance getHzInstance() {
        return hzInstance;
    }
}
