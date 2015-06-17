package ro.fortsoft.hztask.master.topology;

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

    private static final int MAX_ASK_ATTEMPTS = 5;

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

    /**
     * Start the procedure to prepare a new member of the cluster to become an Agent of the
     * current Master
     * @param member new member
     */
    public void startJoinProtocolFor(Member member) {
        askMemberIsReadyToJoin(member, 0);
    }

    private void askMemberIsReadyToJoin(Member member, int attempt) {
        if(attempt > MAX_ASK_ATTEMPTS) {
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

    private void askAgentToAcceptMaster(Member member, int attempt) {
        if(attempt > MAX_ASK_ATTEMPTS) {
            log.error("NOT_ACCEPT_MASTER_AFTER_MANY_ATTEMPTS: After {} attempts the member {} " +
                    "did not respond affirmatively", attempt);
            return;
        }

        try {
            log.info("Asking {} if ACCEPT MASTER attempt {}", member, attempt);
            communicationService.submitToMember(member, new AnnounceMasterAndSignalStartWorkOp(getMaster()),
                     new AnnounceMasterCallback(member, attempt));
        } catch (Exception e) {
            log.error("Error sending AnnounceMasterAndSignalStartWorkOp", e);
        }
    }



    public void addAgent(Member member) {
        agents.add(member);
        String name = member.getStringAttribute(HzKeysConstants.AGENT_NAME_PROPERTY);
        if(name != null) {
            NamesUtil.addMember(member.getUuid(), name);
        }
        log.info("Added cluster agent {}", NamesUtil.getName(member.getUuid()));
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


    public Member getLocalMember() {
        return hzInstance.getCluster().getLocalMember();
    }

    public HazelcastInstance getHzInstance() {
        return hzInstance;
    }

    /**
     * Trigger on the Agents the message to output debug statistics
     */
    public void sendOutputDebugStatsToMember() {
        List<Member> agents = getAgentsCopy();
        for(Member member : agents) {
            communicationService.sendOutputDebugStatsMessageToMember(member);
        }
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
                askAgentToAcceptMaster(member, 0);
            } else {
                TimerTask timerTask = new TimerTask() {
                    @Override
                    public void run() {
                        askMemberIsReadyToJoin(member, ++ attempt);
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


    private class AnnounceMasterCallback implements ExecutionCallback<Boolean> {

        private Member member;
        private int attempt;

        private AnnounceMasterCallback(Member member, int attempt) {
            this.member = member;
            this.attempt = attempt;
        }

        @Override
        public void onResponse(Boolean response) {
            if(response) {
                log.info("NEW_JOIN New cluster agent {} ID={} is active", member, member.getUuid());
                eventBus.post(new AgentJoinedEvent(member));
            } else {
                TimerTask timerTask = new TimerTask() {
                    @Override
                    public void run() {
                        askAgentToAcceptMaster(member, ++ attempt);
                    }
                };
                new Timer().schedule(timerTask, 5000);
            }
        }

        @Override
        public void onFailure(Throwable t) {
            log.error("EXCEPTION_ON_MASTER_ANNOUNCE - Agent responded with failure", t);
        }
    }
}
