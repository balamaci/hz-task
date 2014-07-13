/*
 * Copyright (c) 2013 Serban Balamaci
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.fortsoft.hztask;

import com.fortsoft.hztask.agent.consumer.TaskConsumerThread;
import com.fortsoft.hztask.cluster.IClusterAgentService;
import com.fortsoft.hztask.util.ClusterUtil;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Executors;

/**
 * @author Serban Balamaci
 */
public class ClusterAgent implements IClusterAgentService {

    private static final Logger log = LoggerFactory.getLogger(ClusterAgent.class);

    private HazelcastInstance hzInstance;

    private Map<Class, TaskProcessorFactory> processorRegistry;

    public final int maxRunningTasks = 10;

    private TaskConsumerThread taskConsumerThread;

    private Member master;

    /** Pool of threads that handle the
     * task processing
     */
    private ListeningExecutorService taskExecutorService = MoreExecutors.
            listeningDecorator(Executors.newFixedThreadPool(maxRunningTasks));

    public ClusterAgent(Map<Class, TaskProcessorFactory> processorRegistry, String configXmlFileName) {
        this.processorRegistry = processorRegistry;

        log.info("Starting agent");

        Config cfg = new ClasspathXmlConfig(configXmlFileName);

        hzInstance = Hazelcast.newHazelcastInstance(cfg);

        hzInstance.getConfig().getUserContext().put("clusterAgentService", this);
//        tasks.addEntryListener(new TaskEntryListener(this),
//                new SqlPredicate("clusterInstanceUuid=" + localUUID), true);
    }

    public Map<Class, TaskProcessorFactory> getProcessorRegistry() {
        return processorRegistry;
    }

    public HazelcastInstance getHzInstance() {
        return hzInstance;
    }

    public TaskConsumerThread getTaskConsumerThread() {
        return taskConsumerThread;
    }

    public void announceMaster(String masterUuid) {
        System.out.println("Announcing master");
        master = ClusterUtil.findMemberWithUuid(hzInstance, masterUuid).get();
        if(taskConsumerThread == null) {
            startTaskConsumer();
        }
    }

    public Member getMaster() {
        return master;
    }

    private void startTaskConsumer() {
        taskConsumerThread = new TaskConsumerThread(this);

        taskConsumerThread.start();
    }

    public ListeningExecutorService getTaskExecutorService() {
        return taskExecutorService;
    }



    @Override
    public boolean isActive() {
        System.out.println("Called is active");
        return taskConsumerThread != null && taskConsumerThread.isAlive();

    }

    public int getMaxRunningTasks() {
        return maxRunningTasks;
    }
}
