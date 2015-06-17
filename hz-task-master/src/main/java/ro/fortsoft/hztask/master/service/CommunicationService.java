package ro.fortsoft.hztask.master.service;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.Member;
import ro.fortsoft.hztask.common.HzKeysConstants;
import ro.fortsoft.hztask.op.AbstractClusterOp;
import ro.fortsoft.hztask.op.agent.OutputDebugStatsOp;
import ro.fortsoft.hztask.op.agent.ShutdownAgentOp;

import java.util.concurrent.Future;

/**
 * Messaging backend between the Master and Agents through which the master sends explicits messages
 *
 * @author Serban Balamaci
 */
public class CommunicationService {

    private final IExecutorService communicationExecutorService;


    public CommunicationService(HazelcastInstance hzInstance) {
        communicationExecutorService = hzInstance.getExecutorService(HzKeysConstants.EXECUTOR_SERVICE_COMS);

    }

    public <T> Future<T> sendMessageToMember(Member member, AbstractClusterOp<T> op) {
        return communicationExecutorService.submitToMember(op, member);
    }

    public void submitToMember(Member member, AbstractClusterOp op,
                                        ExecutionCallback executionCallback) {
        communicationExecutorService.submitToMember(op, member, executionCallback);
    }

    public Future sendShutdownMessageToMember(Member member) {
        return communicationExecutorService.submitToMember(new ShutdownAgentOp(), member);
    }

    public Future sendOutputDebugStatsMessageToMember(Member member) {
        return communicationExecutorService.submitToMember(new OutputDebugStatsOp(), member);
    }

}
