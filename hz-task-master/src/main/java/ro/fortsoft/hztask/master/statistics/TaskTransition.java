package ro.fortsoft.hztask.master.statistics;

import java.util.Date;

/**
 * Describes a Task transition who processed the tasks and when
 *
 * @author Serban Balamaci
 */
public class TaskTransition {

    private final TaskStatus type;
    private final Date eventDate;
    private String memberId;

    private TaskTransition(TaskStatus type, String memberId) {
        this.type = type;
        this.eventDate = new Date();
        this.memberId = memberId;
    }

    public static TaskTransition create(TaskStatus type) {
        return new TaskTransition(type, null);
    }

    public static TaskTransition create(TaskStatus type, String memberId) {
        return new TaskTransition(type, memberId);
    }

    public TaskStatus getType() {
        return type;
    }

    public Date getEventDate() {
        return eventDate;
    }

    public String getMemberId() {
        return memberId;
    }
}
