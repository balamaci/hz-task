package ro.fortsoft.hztask.master.statistics;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Multimap;

import static ro.fortsoft.hztask.master.statistics.TaskStatus.*;
import static ro.fortsoft.hztask.master.statistics.TaskTransition.create;

/**
 * Keeps track of state transitions for tasks, doesn't play a role in the
 * workings of task distribution, but helps debug what went wrong
 *
 * @author Serban Balamaci
 */
public class TaskTransitionLogKeeper {

    private Multimap<String, TaskTransition> tracker = ArrayListMultimap.create();

    public void taskReceived(String id) {
        logTaskActivity(id, CREATED);
    }

    public void taskAssigned(String id, String memberId) {
        logTaskActivity(id, ASSIGNED, memberId);
    }

    public void taskUnassigned(String id) {
        logTaskActivity(id, UNASSIGNED);
    }

    public void taskReassigned(String id, String memberId) {
        logTaskActivity(id, TaskStatus.REASSIGNED, memberId);
    }

    public synchronized void taskFinishedSuccess(String id) {
        tracker.removeAll(id);
    }

    public synchronized void taskFinishedFailure(String id) {
        tracker.removeAll(id);
    }

    private synchronized void addActivity(String id, TaskTransition entry) {
        tracker.put(id, entry);
    }

    private void logTaskActivity(String id, TaskStatus taskStatus) {
        TaskTransition entry = create(taskStatus);
        addActivity(id, entry);
    }

    private void logTaskActivity(String id, TaskStatus taskStatus, String memberId) {
        TaskTransition entry = create(taskStatus, memberId);
        addActivity(id, entry);
    }

    public synchronized ImmutableListMultimap<String, TaskTransition> getDataCopy() {
        return ImmutableListMultimap.copyOf(tracker);
    }

}
