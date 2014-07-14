package com.fortsoft.hztask.master.scheduler;

import com.fortsoft.hztask.master.ClusterMasterServiceImpl;

/**
 * @author Serban Balamaci
 */
public class UnassignedTaskScheduler implements Runnable {

    private ClusterMasterServiceImpl clusterMasterService;

    @Override
    public void run() {
        clusterMasterService.rescheduleUnassignedTasks();
    }
}
