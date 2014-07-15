package ro.fortsoft.hztask.master.listener;

import ro.fortsoft.hztask.common.task.Task;

/**
 * @author Serban Balamaci
 */
public abstract class TaskCompletionListener {

    public abstract void onSuccess(Task task, Object taskResult);

    public abstract void onFail(Task task, Throwable throwable);


}
