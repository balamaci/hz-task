package ro.fortsoft.hztask.master.statistics;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static ro.fortsoft.hztask.master.statistics.TaskStatus.ASSIGNED;
import static ro.fortsoft.hztask.master.statistics.TaskStatus.CREATED;
import static ro.fortsoft.hztask.master.statistics.TaskStatus.UNASSIGNED;
import static ro.fortsoft.hztask.master.statistics.TaskTransition.create;

/**
 * Keeps track of state transitions for tasks, doesn't play a role in the
 * workings of task distribution, but helps debug what went wrong
 *
 * @author Serban Balamaci
 */
public class TaskTransitionLogKeeper {

    private Map<String, List<TaskTransition>> tracker = new ConcurrentHashMap<>();

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

    public void taskFinishedSuccess(String id) {
        tracker.remove(id);
    }

    public void taskFinishedFailure(String id) {
        tracker.remove(id);
    }

    private void addActivity(String id, TaskTransition entry) {
        List<TaskTransition> transitions  = tracker.computeIfAbsent(id, key -> new CopyOnWriteArrayList<>());
        transitions.add(entry);
    }

    private void logTaskActivity(String id, TaskStatus taskStatus) {
        TaskTransition entry = create(taskStatus);
        addActivity(id, entry);
    }

    private void logTaskActivity(String id, TaskStatus taskStatus, String memberId) {
        TaskTransition entry = create(taskStatus, memberId);
        addActivity(id, entry);
    }

    public Map<String, List<TaskTransition>> getDataCopy() {
        return new HashMap<>(tracker);
    }

}
