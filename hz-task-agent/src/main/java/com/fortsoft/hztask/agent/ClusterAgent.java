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

package com.fortsoft.hztask.agent;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Serban Balamaci
 */
public class ClusterAgent  {

    private static final Logger log = LoggerFactory.getLogger(ClusterAgent.class);

    public ClusterAgent(AgentConfig config, Config hzConfig) {
        log.info("Starting agent ...");

        HazelcastInstance hzInstance = Hazelcast.newHazelcastInstance(hzConfig);

        log.info("Starting Agent with ClusterID {}", hzInstance.getCluster().getLocalMember().getUuid());

        ClusterAgentServiceImpl clusterAgentService = new ClusterAgentServiceImpl(config);
        clusterAgentService.setHzInstance(hzInstance);

        hzInstance.getConfig().getUserContext().put("clusterAgentService", clusterAgentService);
//        tasks.addEntryListener(new TaskEntryListener(this),
//                new SqlPredicate("clusterInstanceUuid=" + localUUID), true);
    }

}
