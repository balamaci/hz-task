package ro.fortsoft.hztask.master.util;

import com.hazelcast.config.Config;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;

/**
 * @author Serban Balamaci
 */
public class ConfigUtil {

    private static final int TASK_NOTIFICATION_POOL_SIZE = 16;
    private static final int COMS_POOL_SIZE = 16;

    public static Config addInternalConfig(Config config, int backupCount) {
        MapConfig tasksConfig = new MapConfig("tasks");
        tasksConfig.setAsyncBackupCount(0);
        tasksConfig.setBackupCount(backupCount);
        tasksConfig.setInMemoryFormat(InMemoryFormat.BINARY);
        tasksConfig.setTimeToLiveSeconds(0);
        tasksConfig.setMaxIdleSeconds(0);


        ExecutorConfig tasksNotificationExecutor = new ExecutorConfig("ro.fortsoft.finishedTasks");
        tasksNotificationExecutor.setPoolSize(TASK_NOTIFICATION_POOL_SIZE);
        tasksNotificationExecutor.setQueueCapacity(0); //unbounded

        ExecutorConfig communicationExecutor = new ExecutorConfig("ro.fortsoft.coms");
        communicationExecutor.setPoolSize(COMS_POOL_SIZE);
        communicationExecutor.setQueueCapacity(0); //unbounded

        config.addExecutorConfig(tasksNotificationExecutor);
        config.addExecutorConfig(communicationExecutor);
        config.addMapConfig(tasksConfig);

        return config;
    }


}
