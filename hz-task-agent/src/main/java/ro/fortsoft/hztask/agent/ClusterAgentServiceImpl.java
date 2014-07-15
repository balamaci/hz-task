package ro.fortsoft.hztask.agent;

import ro.fortsoft.hztask.agent.consumer.TaskConsumerThread;
import ro.fortsoft.hztask.agent.processor.TaskProcessorFactory;
import ro.fortsoft.hztask.cluster.IClusterAgentService;
import ro.fortsoft.hztask.util.ClusterUtil;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;

import java.util.Map;
import java.util.concurrent.Executors;

/**
 * @author Serban Balamaci
 */
public class ClusterAgentServiceImpl implements IClusterAgentService {

    private TaskConsumerThread taskConsumerThread;

    private HazelcastInstance hzInstance;

    private AgentConfig config;

    private Member master;

    /**
     * Pool of threads that handle the
     * task processing
     */
    private ListeningExecutorService taskExecutorService;

    public ClusterAgentServiceImpl(AgentConfig agentConfig) {
        this.config = agentConfig;
        taskExecutorService = MoreExecutors.
                listeningDecorator(Executors.newFixedThreadPool(agentConfig.getMaxRunningTasks()));
    }

    @Override
    public boolean isActive() {
        return taskConsumerThread != null && taskConsumerThread.isAlive();
    }

    @Override
    public void announceMaster(String masterUuid) {
        master = ClusterUtil.findMemberWithUuid(hzInstance, masterUuid).get();
        if (taskConsumerThread == null) {
            startTaskConsumer();
        }
    }

    @Override
    public void shutdown() {
        taskConsumerThread.setShuttingDown(true);
        taskConsumerThread.interrupt();

        hzInstance.shutdown();
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

    public ListeningExecutorService getTaskExecutorService() {
        return taskExecutorService;
    }

    public Member getMaster() {
        return master;
    }

    public int getMaxRunningTasks() {
        return config.getMaxRunningTasks();
    }
}