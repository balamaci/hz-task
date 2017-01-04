package ro.fortsoft.hztask.comparator;

import ro.fortsoft.hztask.common.task.TaskKey;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Map;

/**
 * Hazelcast because we use only makes available the key values in the comparator
 *
 * @author Serban Balamaci
 */
public class PriorityAndOldestTaskComparator implements Comparator<Map.Entry>, Serializable {

    //TODO implement priority related with KEY or find a solution
    @Override
    public int compare(Map.Entry e1, Map.Entry e2) {
        TaskKey key1 = (TaskKey) e1.getKey();
        TaskKey key2 = (TaskKey) e2.getKey();

//        if(task1.getPriority() == task2.getPriority()) {
            return key1.getTaskNo() <= key2.getTaskNo() ? -1 : 1;
//        }

//        return task1.getPriority() < task2.getPriority() ? -1 : 1;
    }
}
