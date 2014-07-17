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

package ro.fortsoft.hztask.agent;

import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.EventBus;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ro.fortsoft.hztask.agent.event.task.TaskEventSubscriber;
import ro.fortsoft.hztask.agent.listener.ClusterMembershipListener;
import ro.fortsoft.hztask.common.HzKeysConstants;
import ro.fortsoft.hztask.common.MemberType;

import java.util.concurrent.Executors;

/**
 * @author Serban Balamaci
 */
public class ClusterAgent  {

    private static final Logger log = LoggerFactory.getLogger(ClusterAgent.class);

    private EventBus eventBus = new AsyncEventBus(Executors.newCachedThreadPool());

    private ClusterAgentService clusterAgentService;

    public ClusterAgent(AgentConfig config, Config hzConfig) {
        log.info("Starting agent ...");

        HazelcastInstance hzInstance = Hazelcast.newHazelcastInstance(hzConfig);
        hzInstance.getUserContext().put(HzKeysConstants.USER_CONTEXT_MEMBER_TYPE, MemberType.AGENT);

        log.info("Starting Agent with ClusterID {}", hzInstance.getCluster().getLocalMember().getUuid());

        clusterAgentService = new ClusterAgentService(config, eventBus);
        clusterAgentService.setHzInstance(hzInstance);

        hzInstance.getUserContext().put(HzKeysConstants.USER_CONTEXT_CLUSTER_AGENT_SERVICE,
                clusterAgentService);
        hzInstance.getCluster().addMembershipListener(new ClusterMembershipListener(clusterAgentService));

        registerEventListeners();
    }

    private void registerEventListeners() {
        eventBus.register(new TaskEventSubscriber(clusterAgentService));
    }

}
