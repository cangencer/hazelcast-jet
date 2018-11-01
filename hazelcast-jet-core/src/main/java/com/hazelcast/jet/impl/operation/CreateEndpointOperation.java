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

package com.hazelcast.jet.impl.operation;

import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.impl.JetService;
import com.hazelcast.jet.pipeline.ContextFactory;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class CreateEndpointOperation extends Operation {

    private long endpointId;
    private String name;
    private ContextFactory contextFactory;
    private DistributedBiFunction<Object, Object, CompletableFuture<Object>> handler;

    public CreateEndpointOperation() {
    }

    public CreateEndpointOperation(long endpointId, String name, ContextFactory contextFactory,
                                   DistributedBiFunction<Object, Object, CompletableFuture<Object>> handler) {
        this.endpointId = endpointId;
        this.name = name;
        this.contextFactory = contextFactory;
        this.handler = handler;
    }

    @Override
    public String getServiceName() {
        return JetService.SERVICE_NAME;
    }

    @Override
    public void run() throws Exception {
        JetService service = getService();
        service.getEndpointService().newEndpoint(endpointId, name, contextFactory, handler);
        super.run();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeLong(endpointId);
        out.writeUTF(name);
        out.writeObject(contextFactory);
        out.writeObject(handler);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        endpointId = in.readLong();
        name = in.readUTF();
        contextFactory = in.readObject();
        handler = in.readObject();
    }
}
