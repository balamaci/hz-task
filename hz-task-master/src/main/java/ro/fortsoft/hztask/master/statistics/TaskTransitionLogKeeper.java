package ro.fortsoft.hztask.master.statistics;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Multimap;

import static ro.fortsoft.hztask.master.statistics.TaskActivity.*;
import static ro.fortsoft.hztask.master.statistics.TaskActivityEntry.create;

/**
 * Keeps track of state transitions for tasks, doesn't play a role in the
 * workings of task distribution, but helps debug what went wrong
 *
 * @author Serban Balamaci
 */
public class TaskTransitionLogKeeper {

    private Multimap<String, TaskActivityEntry> tracker = ArrayListMultimap.create();

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
        logTaskActivity(id, TaskActivity.REASSIGNED, memberId);
    }

    public synchronized void taskFinishedSuccess(String id) {
        tracker.removeAll(id);
    }

    public synchronized void taskFinishedFailure(String id) {
        tracker.removeAll(id);
    }

    private synchronized void addActivity(String id, TaskActivityEntry entry) {
        tracker.put(id, entry);
    }

    private void logTaskActivity(String id, TaskActivity taskActivity) {
        TaskActivityEntry entry = create(taskActivity);
        addActivity(id, entry);
    }

    private void logTaskActivity(String id, TaskActivity taskActivity, String memberId) {
        TaskActivityEntry entry = create(taskActivity, memberId);
        addActivity(id, entry);
    }

    public synchronized ImmutableListMultimap<String, TaskActivityEntry> getDataCopy() {
        return ImmutableListMultimap.copyOf(tracker);
    }

}
