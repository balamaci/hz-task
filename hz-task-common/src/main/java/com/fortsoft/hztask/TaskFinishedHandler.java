package com.fortsoft.hztask;

import com.fortsoft.hztask.common.task.TaskKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Called on the master to handle the logic related to handling
 * the completion of a task when an agent calls back with the task result.
 *
 * @author Serban Balamaci
 */
public class TaskFinishedHandler {

    private static Logger log = LoggerFactory.getLogger(TaskFinishedHandler.class);

    private TaskFinishedHandler() {
    }


    public void notifyTaskFinishedSuccessfully(TaskKey taskKey, Object response) {
        log.info("Master received task finished succesfully");
    }

}
