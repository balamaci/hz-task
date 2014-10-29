package ro.fortsoft.hztask.master.statistics;

import java.util.Date;

/**
 * @author Serban Balamaci
 */
public class TaskActivityEntry {

    private final TaskActivity type;
    private final Date eventDate;
    private String memberId;

    private TaskActivityEntry(TaskActivity type, String memberId) {
        this.type = type;
        this.eventDate = new Date();
        this.memberId = memberId;
    }

    public static TaskActivityEntry create(TaskActivity type) {
        return new TaskActivityEntry(type, null);
    }

    public static TaskActivityEntry create(TaskActivity type, String memberId) {
        return new TaskActivityEntry(type, memberId);
    }

    public TaskActivity getType() {
        return type;
    }

    public Date getEventDate() {
        return eventDate;
    }

    public String getMemberId() {
        return memberId;
    }
}
