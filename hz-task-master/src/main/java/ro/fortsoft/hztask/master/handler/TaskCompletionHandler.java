package ro.fortsoft.hztask.master.handler;

import ro.fortsoft.hztask.common.task.Task;

/**
 * @author Serban Balamaci
 */
public interface TaskCompletionHandler<T extends Task> {

    public abstract void onSuccess(T task, Object taskResult, String agentName);

    public abstract void onFail(T task, Throwable throwable, String agentName);


}
