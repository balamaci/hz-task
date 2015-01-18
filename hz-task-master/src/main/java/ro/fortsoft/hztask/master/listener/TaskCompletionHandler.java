package ro.fortsoft.hztask.master.listener;

import ro.fortsoft.hztask.common.task.Task;

/**
 * @author Serban Balamaci
 */
public abstract class TaskCompletionHandler {

    public abstract void onSuccess(Task task, Object taskResult, String agentName);

    public abstract void onFail(Task task, Throwable throwable, String agentName);


}
