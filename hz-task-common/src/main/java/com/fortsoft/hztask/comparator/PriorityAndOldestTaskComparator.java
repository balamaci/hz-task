package com.fortsoft.hztask.comparator;

import com.fortsoft.hztask.common.task.Task;
import com.fortsoft.hztask.common.task.TaskKey;

import java.util.Comparator;
import java.util.Map;

/**
 * @author Serban Balamaci
 */
public class PriorityAndOldestTaskComparator implements Comparator<Map.Entry<TaskKey, Task>> {
    @Override
    public int compare(Map.Entry<TaskKey, Task> e1, Map.Entry<TaskKey, Task> e2) {
        Task task1 = e1.getValue();
        Task task2 = e2.getValue();

        if(task1.getPriority() == task2.getPriority()) {
            return task1.getCreationDate() <= task2.getCreationDate() ? -1 : 1;
        }

        return task1.getPriority() < task2.getPriority() ? -1 : 1;
    }
}
