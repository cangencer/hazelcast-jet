/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet;

import com.hazelcast.config.NetworkConfig;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.datamodel.Tuple2;
import org.junit.Before;
import org.junit.Test;

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;

public class EndpointTest {

    private JetInstance instance;
    private JetInstance liteMember;

    @Before
    public void setup() {
        JetConfig memberConfig = new JetConfig();
        NetworkConfig nwConfig = memberConfig.getHazelcastConfig().getNetworkConfig();
        nwConfig.getJoin().getMulticastConfig().setEnabled(false);
        nwConfig.getJoin().getTcpIpConfig().setEnabled(true);
        nwConfig.getJoin().getTcpIpConfig().addMember("127.0.0.1");
        memberConfig.getHazelcastConfig().setNetworkConfig(nwConfig);
        instance = Jet.newJetInstance(memberConfig);

        JetConfig liteMemberConfig = new JetConfig();
        liteMemberConfig.getHazelcastConfig().setLiteMember(true);
        liteMemberConfig.getHazelcastConfig().setNetworkConfig(nwConfig);
        liteMember = Jet.newJetInstance(liteMemberConfig);

        instance.<Tuple2<Integer, Integer>, Integer>newEndpoint("sum", (t, f) -> f.complete(t.f0() + t.f1()));
    }

    @Test
    public void endpoint() {
        IEndpoint<Tuple2<Integer, Integer>, Integer> endpoint = liteMember.getEndpoint("sum");
        endpoint.call(tuple2(1, 2));
    }
}
