/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.jeton;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import py4j.GatewayServer;

import java.util.List;

public class JetonGateway {

    private final JetInstance jet;

    public JetonGateway() {
            jet = Jet.newJetInstance();
        }

        public JetInstance getJetInstance() {
            return jet;
        }

        public void execute(List<Transform> transforms) {
            for (Transform transform : transforms) {
                System.out.println(transform);
            }
        }

        public static void main(String[] args) {
            GatewayServer gatewayServer = new GatewayServer(new JetonGateway());
            gatewayServer.start();
            System.out.println("Gateway Server Started");
        }

    }
