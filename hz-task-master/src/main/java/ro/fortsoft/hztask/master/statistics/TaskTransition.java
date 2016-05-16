package ro.fortsoft.hztask.master.statistics;

import java.time.LocalDateTime;

/**
 * Describes a Task transition who processed the tasks and when
 *
 * @author Serban Balamaci
 */
public class TaskTransition {

    private final TaskStatus type;
    private final LocalDateTime eventDate;
    private String memberId;

    private TaskTransition(TaskStatus type, String memberId) {
        this.type = type;
        this.eventDate = LocalDateTime.now();
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

    public LocalDateTime getEventDate() {
        return eventDate;
    }

    public String getMemberId() {
        return memberId;
    }
}
