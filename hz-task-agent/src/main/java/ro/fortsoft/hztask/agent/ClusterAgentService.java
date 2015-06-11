package ro.fortsoft.hztask.agent;

import com.google.common.eventbus.EventBus;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ro.fortsoft.hztask.agent.consumer.TaskConsumerThread;
import ro.fortsoft.hztask.agent.processor.TaskProcessorFactory;
import ro.fortsoft.hztask.cluster.IClusterAgentService;
import ro.fortsoft.hztask.util.ClusterUtil;

import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author Serban Balamaci
 */
public class ClusterAgentService implements IClusterAgentService {

    private TaskConsumerThread taskConsumerThread;

    private HazelcastInstance hzInstance;

    private AgentConfig config;

    private Member master;

    private ReentrantReadWriteLock lockMaster = new ReentrantReadWriteLock();

    private EventBus eventBus;

    private static final Logger log = LoggerFactory.getLogger(ClusterAgentService.class);

    public ClusterAgentService(AgentConfig agentConfig, EventBus eventBus) {
        this.config = agentConfig;
        this.eventBus = eventBus;
    }

    @Override
    public boolean isActive() {
        return taskConsumerThread != null && taskConsumerThread.isAlive();
    }

    @Override
    public boolean setMaster(String masterUuid) {
        lockMaster.writeLock().lock();

        try {
            if(master != null) {
                return false;
            }
            master = ClusterUtil.findMemberWithUuid(hzInstance, masterUuid).get();

            return true;
        } finally {
            lockMaster.writeLock().unlock();
        }
    }

    public void handleMasterLeft() {
        log.info("Master has left the cluster!!");
        lockMaster.writeLock().lock();
        try {
            master = null;
            stopWork();
        } finally {
            lockMaster.writeLock().unlock();
        }
    }

    public void stopWork() {
        taskConsumerThread.setShuttingDown(true);
        taskConsumerThread.interrupt();
    }

    @Override
    public void startWork() {
        if (taskConsumerThread == null || ! taskConsumerThread.isAlive()) {
            startTaskConsumer();
        }
    }

    @Override
    public void shutdown() {
        stopWork();

        hzInstance.shutdown();
    }

    @Override
    public void outputDebugStatistics() {
        taskConsumerThread.outputDebugStatistics();
    }

    private void startTaskConsumer() {
        taskConsumerThread = new TaskConsumerThread(this);

        taskConsumerThread.start();
    }

    public TaskConsumerThread getTaskConsumerThread() {
        return taskConsumerThread;
    }

    public HazelcastInstance getHzInstance() {
        return hzInstance;
    }

    public void setHzInstance(HazelcastInstance hzInstance) {
        this.hzInstance = hzInstance;
    }

    public Map<Class, TaskProcessorFactory> getProcessorRegistry() {
        return config.getProcessorRegistry();
    }

    public Member getMaster() {
        lockMaster.readLock().lock();
        try {
            return master;
        } finally {
            lockMaster.readLock().unlock();
        }
    }

    public int getMaxRunningTasks() {
        return config.getMaxRunningTasks();
    }

    public EventBus getEventBus() {
        return eventBus;
    }
}