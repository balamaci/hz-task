package ro.fortsoft.hztask.master.statistics;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Multimap;

import static ro.fortsoft.hztask.master.statistics.TaskActivityEntry.create;

/**
 * Keeps track of state transitions for tasks, doesn't play a role in the
 * workings of task distribution, but helps debug what went wrong
 *
 * @author Serban Balamaci
 */
public class TaskLogKeeper {

    private Multimap<String, TaskActivityEntry> tracker = ArrayListMultimap.create();

    public void taskReceived(String id) {
        TaskActivityEntry entry = create(TaskActivity.CREATED);

        logTaskActivity(id, entry);
    }

    public void taskAssigned(String id, String memberId) {
        TaskActivityEntry entry = create(TaskActivity.ASSIGNED, memberId);

        logTaskActivity(id, entry);
    }

    public void taskUnassigned(String id) {
        TaskActivityEntry entry = create(TaskActivity.UNASSIGNED);

        logTaskActivity(id, entry);
    }

    public void taskReassigned(String id, String memberId) {
        TaskActivityEntry entry = create(TaskActivity.REASSIGNED, memberId);

        logTaskActivity(id, entry);
    }

    public synchronized void taskFinishedSuccess(String id) {
        tracker.removeAll(id);
    }

    public synchronized void taskFinishedFailure(String id) {
        tracker.removeAll(id);
    }

    private synchronized void logTaskActivity(String id, TaskActivityEntry entry) {
        tracker.put(id, entry);
    }

    public synchronized ImmutableListMultimap<String, TaskActivityEntry> getDataCopy() {
        return ImmutableListMultimap.copyOf(tracker);
    }

}
