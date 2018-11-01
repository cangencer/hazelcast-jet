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

import com.hazelcast.core.Member;
import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.jet.IEndpoint;
import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.impl.operation.CreateEndpointOperation;
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.Packet.Type;
import com.hazelcast.spi.NodeEngine;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static com.hazelcast.jet.impl.util.Util.createObjectDataOutput;
import static com.hazelcast.jet.impl.util.Util.getMemberConnection;

public class EndpointProxy<I, O> implements IEndpoint<I, O> {

    private NodeEngine nodeEngine;
    private String name;
    private long endpointId;
    private Connection[] participants;

    private final AtomicLong sequence = new AtomicLong();

    private final ConcurrentMap<Long, CompletableFuture> requests = new ConcurrentHashMap<>();

    public EndpointProxy(NodeEngine nodeEngine, long endpointId, String name) {
        this.nodeEngine = nodeEngine;
        this.endpointId = endpointId;
        this.name = name;
        Collection<Member> members = nodeEngine.getClusterService().getMembers(DATA_MEMBER_SELECTOR);
        participants = members.stream()
                              .filter(m -> !m.localMember())
                              .map(m -> getMemberConnection(nodeEngine, m.getAddress())).toArray(Connection[]::new);
    }

    public EndpointProxy(NodeEngine nodeEngine, String name, DistributedBiConsumer<I, CompletableFuture<O>> handler) {
        this.nodeEngine = nodeEngine;
        this.name = name;
        FlakeIdGenerator idGenerator = nodeEngine.getHazelcastInstance().getFlakeIdGenerator("endpoints");
        Collection<Member> members = nodeEngine.getClusterService().getMembers(DATA_MEMBER_SELECTOR);
        participants = members.stream()
                .filter(m -> !m.localMember())
                .map(m -> getMemberConnection(nodeEngine, m.getAddress())).toArray(Connection[]::new);
        endpointId = idGenerator.newId();
        CreateEndpointOperation op = new CreateEndpointOperation(endpointId, name, handler);
        for (Member member : members) {
            this.nodeEngine.getOperationService()
                           .createInvocationBuilder(JetService.SERVICE_NAME, op, member.getAddress())
                           .invoke().join();
        }
    }

    @Override
    public CompletableFuture<O> callAsync(I request) {
        // pick a member to execute the request on
        long requestId = sequence.getAndIncrement();
        Connection connection = participants[(int) Math.floorMod(requestId, participants.length)];
        CompletableFuture future = new CompletableFuture();
        requests.put(requestId, future);
        try (BufferObjectDataOutput out = createObjectDataOutput(nodeEngine)) {
            out.writeByte(Networking.FLAG_TYPE_RPC_REQUEST);
            out.writeLong(endpointId);
            out.writeLong(requestId);
            out.writeObject(request);

            Packet p = new Packet(out.toByteArray())
                    .setPacketType(Type.JET);
            connection.write(p);
        } catch (IOException e) {
            throw rethrow(e);
        }
        return future;
    }

    @Override
    public void destroy() {

    }

    public long getEndpointId() {
        return endpointId;
    }

    public String getName() {
        return name;
    }

    public void handleResponse(BufferObjectDataInput in) throws IOException {
        long requestId = in.readLong();
        CompletableFuture requestFuture = requests.remove(requestId);
        if (requestFuture == null) {
            throw new IllegalArgumentException("Response for unknown request: " + requestId);
        }
        Object response = in.readObject();
        requestFuture.complete(response);
    }
}
