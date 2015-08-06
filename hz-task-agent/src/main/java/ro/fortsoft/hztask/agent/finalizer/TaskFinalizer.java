package ro.fortsoft.hztask.agent.finalizer;

import com.google.common.base.Optional;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 *
 *
 * @author sbalamaci
 */
public class TaskFinalizer {

    private ClusterAgentService clusterAgentService;

    private static final Logger log = LoggerFactory.getLogger(TaskFinalizer.class);

    public TaskFinalizer(ClusterAgentService clusterAgentService) {
        this.clusterAgentService = clusterAgentService;
    }

    public void notifyTaskFailed(TaskKey taskKey, Throwable exception) {
        HazelcastInstance hzInstance = clusterAgentService.getHzInstance();

        Optional<Future> masterNotified = sendToMaster(new NotifyMasterTaskFailedOp(taskKey, exception,
                ClusterUtil.getLocalMemberUuid(hzInstance)));
        waitForFutureToComplete(masterNotified);

        clusterAgentService.getTaskConsumerThread().removeFromRunningTasksQueue(taskKey);
    }

    public void notifyTaskFinished(TaskKey taskKey, Serializable result) {
        HazelcastInstance hzInstance = clusterAgentService.getHzInstance();

        log.info("Notifying Master of task {} finished successfully", taskKey.getTaskId());
        Optional<Future> masterNotified = sendToMaster(new NotifyMasterTaskFinishedOp(taskKey, result,
                ClusterUtil.getLocalMemberUuid(hzInstance)));
        waitForFutureToComplete(masterNotified);

        clusterAgentService.getTaskConsumerThread().removeFromRunningTasksQueue(taskKey);
    }

    private void waitForFutureToComplete(Optional<Future> futureOptional) {
        if(futureOptional.isPresent()) {
            try {
                futureOptional.get().get();
            } catch (InterruptedException e) {
                log.info("Callback for Master notification received an interrupt signal, stopping");
            } catch (ExecutionException e) {
                log.error("Task {}, failed to notify Master of it's status", e);
            }
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
            return Optional.absent();
        }
    }

}
