package ro.fortsoft.hztask.agent.finalizer;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.Member;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ro.fortsoft.hztask.agent.ClusterAgentService;
import ro.fortsoft.hztask.common.HzKeysConstants;
import ro.fortsoft.hztask.common.task.TaskKey;
import ro.fortsoft.hztask.op.NotifyMasterTaskFailedOp;
import ro.fortsoft.hztask.op.NotifyMasterTaskFinishedOp;
import ro.fortsoft.hztask.op.master.AbstractMasterOp;
import ro.fortsoft.hztask.util.ClusterUtil;

import java.io.Serializable;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 *
 * @author sbalamaci
 */
public class TaskFinishedHandler {

    private ClusterAgentService clusterAgentService;

    private static final Logger log = LoggerFactory.getLogger(TaskFinishedHandler.class);

    public TaskFinishedHandler(ClusterAgentService clusterAgentService) {
        this.clusterAgentService = clusterAgentService;
    }

    public void failure(TaskKey taskKey, Throwable exception) {
        HazelcastInstance hzInstance = clusterAgentService.getHzInstance();

        Optional<Future> masterNotified = sendToMaster(new NotifyMasterTaskFailedOp(taskKey, exception,
                ClusterUtil.getLocalMemberUuid(hzInstance)));
        if(masterNotified.isPresent()) {
            waitForConfirmationFromMaster(taskKey, masterNotified.get());
        }
        clusterAgentService.getTaskConsumerThread().removeFromRunningTasksQueue(taskKey);
    }

    public void success(TaskKey taskKey, Serializable result) {
        HazelcastInstance hzInstance = clusterAgentService.getHzInstance();

        Optional<Future> masterNotified = sendToMaster(new NotifyMasterTaskFinishedOp(taskKey, result,
                ClusterUtil.getLocalMemberUuid(hzInstance)));
        if(masterNotified.isPresent()) {
            waitForConfirmationFromMaster(taskKey, masterNotified.get());
        }

        clusterAgentService.getTaskConsumerThread().removeFromRunningTasksQueue(taskKey);
    }

    private void waitForConfirmationFromMaster(TaskKey taskKey, Future masterNotification) {
        try {
            masterNotification.get();
        } catch (InterruptedException e) {
            log.info("Callback for Master notification received an interrupt signal, stopping");
        } catch (ExecutionException e) {
            log.error("Task " + taskKey.getTaskId() + " - failed to notify Master of it's status", e);
        }
    }

    private Optional<Future> sendToMaster(AbstractMasterOp masterOp) {
        HazelcastInstance hzInstance = clusterAgentService.getHzInstance();
        IExecutorService executorService = hzInstance.getExecutorService(HzKeysConstants.
                EXECUTOR_SERVICE_FINISHED_TASKS);

        Member master = clusterAgentService.getMaster();
        if(master != null) {
            return Optional.of(executorService.submitToMember(masterOp, master));
        } else {
            log.info("Wanted to notify Master but Master left"); //maybe
            return Optional.empty();
        }
    }

}
