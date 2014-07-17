package ro.fortsoft.hztask.comparator;

import ro.fortsoft.hztask.common.task.Task;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Map;

/**
 * @author Serban Balamaci
 */
public class PriorityAndOldestTaskComparator implements Comparator<Map.Entry>, Serializable {

    @Override
    public int compare(Map.Entry e1, Map.Entry e2) {
        Task task1 = (Task) e1.getValue();
        Task task2 = (Task) e2.getValue();

        if(task1.getPriority() == task2.getPriority()) {
            return task1.getInternalCounter() <= task2.getInternalCounter() ? -1 : 1;
        }

        return task1.getPriority() < task2.getPriority() ? -1 : 1;
    }
}
