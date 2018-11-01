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

package com.hazelcast.jet.impl;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.impl.execution.TaskletExecutionService;
import com.hazelcast.jet.pipeline.ContextFactory;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.BufferObjectDataInput;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

public class EndpointService {

    private final TaskletExecutionService taskletExecutionService;

    private final ConcurrentMap<String, Long> nameToIds = new ConcurrentHashMap<>();
    // for server
    private final ConcurrentMap<Long, EndpointContext> endpoints = new ConcurrentHashMap<>();
    // for client
    private final ConcurrentMap<Long, EndpointProxy> proxies = new ConcurrentHashMap<>();
    private final JetInstance jetInstance;

    private Networking networking;

    EndpointService(JetService jetService) {
        taskletExecutionService = jetService.getTaskletExecutionService();
        networking = jetService.getNetworking();
        jetInstance = jetService.getJetInstance();
    }

    public void newEndpoint(long id, String name, ContextFactory contextFactory,
                            DistributedBiFunction<Object, Object, CompletableFuture<Object>> handler) {
        if (nameToIds.putIfAbsent(name, id) != null) {
            throw new IllegalArgumentException("Duplicate name " + name);
        }
        endpoints.putIfAbsent(id, new EndpointContext(name, id, contextFactory, handler, taskletExecutionService, networking, jetInstance));
    }

    public long getEndpointId(String name) {
        return nameToIds.get(name);
    }

    public EndpointProxy getOrRegisterProxy(long endpointId, Supplier<EndpointProxy> supplier) {
        return proxies.computeIfAbsent(endpointId, (k) -> supplier.get());
    }

    public EndpointProxy getProxy(long endpointId) {
        return proxies.get(endpointId);
    }

    public void registerProxy(EndpointProxy proxy) {
        proxies.putIfAbsent(proxy.getEndpointId(), proxy);
    }

    public void execute(Address caller, BufferObjectDataInput in) throws IOException {
        long endpointId = in.readLong();
        EndpointContext endpointContext = endpoints.get(endpointId);
        if (endpointContext == null) {
            throw new IllegalArgumentException("Unknown endpoint: " + endpointId);
        }
        endpointContext.handleRequest(caller, in);
    }

    public void setNetworking(Networking networking) {
        this.networking = networking;
    }
}
