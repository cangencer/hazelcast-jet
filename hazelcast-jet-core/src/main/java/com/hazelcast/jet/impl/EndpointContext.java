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

import com.hazelcast.internal.util.concurrent.MPSCQueue;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.impl.execution.Tasklet;
import com.hazelcast.jet.impl.execution.TaskletExecutionService;
import com.hazelcast.jet.impl.util.ProgressState;
import com.hazelcast.jet.pipeline.ContextFactory;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.BufferObjectDataInput;

import java.io.IOException;
import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static com.hazelcast.jet.impl.util.ProgressState.MADE_PROGRESS;
import static com.hazelcast.jet.impl.util.ProgressState.NO_PROGRESS;
import static java.util.Arrays.asList;

public class EndpointContext {
    private final String name;
    private final JetInstance jetInstance;
    private long endpointId;
    private final ContextFactory contextFactory;
    private final DistributedBiFunction<Object, Object, CompletableFuture<Object>> handler;
    private final Queue<Tuple2<Address, BufferObjectDataInput>>[] queues;
    private final Networking networking;
    private AtomicInteger queueIndex = new AtomicInteger();

    public EndpointContext(String name,
                           long endpointId,
                           ContextFactory contextFactory,
                           DistributedBiFunction<Object, Object, CompletableFuture<Object>> handler,
                           TaskletExecutionService taskletExecutionService,
                           Networking networking,
                           JetInstance jetInstance) {
        this.name = name;
        this.endpointId = endpointId;
        this.contextFactory = contextFactory;
        this.handler = handler;
        this.jetInstance = jetInstance;

        queues = new MPSCQueue[taskletExecutionService.cooperativeThreadCount()];
        this.networking = networking;
        Arrays.setAll(queues, i -> new MPSCQueue<>(null));
        EndpointTasklet[] tasklets = new EndpointTasklet[taskletExecutionService.cooperativeThreadCount()];
        Arrays.setAll(tasklets, i -> new EndpointTasklet(queues[i]));
        taskletExecutionService.beginExecute(asList(tasklets), new CompletableFuture<>(), Thread.currentThread().getContextClassLoader());
    }

    public void handleRequest(Address caller, BufferObjectDataInput in) {
        int i = queueIndex.getAndIncrement() % queues.length;
        queues[i].add(tuple2(caller, in));
    }

    public class EndpointTasklet implements Tasklet {

        private final Queue<Tuple2<Address, BufferObjectDataInput>> queue;
        private Object context;

        public EndpointTasklet(Queue<Tuple2<Address, BufferObjectDataInput>> queue) {
            this.queue = queue;
            context = contextFactory.createFn().apply(jetInstance);
        }

        public ProgressState call() {
            Tuple2<Address, BufferObjectDataInput> inputItem = queue.poll();
            if (inputItem == null) {
                return NO_PROGRESS;
            }
            Address caller = inputItem.f0();
            BufferObjectDataInput serialized = inputItem.f1();
            try {
                long requestId = serialized.readLong();
                Object input = serialized.readObject();
                CompletableFuture future = handler.apply(context, input);
                future.whenComplete((r, t) -> {
                    try {
                        networking.sendRpcResponse(caller, endpointId, requestId, t == null ? r : t);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
            } catch (IOException e) {
                throw rethrow(e);
            }
            return MADE_PROGRESS;
        }
    }
}
