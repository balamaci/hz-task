package ro.fortsoft.hztask.master.handler;

import ro.fortsoft.hztask.common.task.Task;

/**
 * Define how to handle a finished Task on the Master
 *
 * @author Serban Balamaci
 */
public interface TaskCompletionHandler<T extends Task> {

    /**
     * How to handle when a Task has finished successfully on an Agent
     * @param task task
     * @param taskResult taskResult
     * @param agentName
     */
    public void onSuccess(T task, Object taskResult, String agentName);

    public void onFail(T task, Throwable throwable, String agentName);

}
