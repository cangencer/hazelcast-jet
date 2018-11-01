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
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.impl.execution.Tasklet;
import com.hazelcast.jet.impl.execution.TaskletExecutionService;
import com.hazelcast.jet.impl.util.ProgressState;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.BufferObjectDataInput;

import java.io.IOException;
import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static com.hazelcast.jet.impl.util.ProgressState.MADE_PROGRESS;
import static com.hazelcast.jet.impl.util.ProgressState.NO_PROGRESS;
import static java.util.Arrays.asList;

public class EndpointContext {
    private final String name;
    private final DistributedBiConsumer<Object, CompletableFuture<Object>> consumer;
    private final Queue<Tuple2<Address, BufferObjectDataInput>>[] queues;
    private final Networking networking;
    private int queueIndex;

    public EndpointContext(String name, DistributedBiConsumer consumer, TaskletExecutionService taskletExecutionService, Networking networking) {
        this.name = name;
        this.consumer = consumer;

        queues = new MPSCQueue[taskletExecutionService.cooperativeThreadCount()];
        this.networking = networking;
        Arrays.setAll(queues, i -> new MPSCQueue<>(null));
        EndpointTasklet[] tasklets = new EndpointTasklet[taskletExecutionService.cooperativeThreadCount()];
        Arrays.setAll(tasklets, i -> new EndpointTasklet(queues[i]));
        taskletExecutionService.beginExecute(asList(tasklets), new CompletableFuture<>(), Thread.currentThread().getContextClassLoader());
    }

    public void handleRequest(Address caller, BufferObjectDataInput in) {
        queues[queueIndex++ % queues.length].add(tuple2(caller, in));
    }

    public class EndpointTasklet implements Tasklet {

        private final Queue<Tuple2<Address, BufferObjectDataInput>> queue;

        public EndpointTasklet(Queue<Tuple2<Address, BufferObjectDataInput>> queue) {
            this.queue = queue;
        }

        public ProgressState call() {
            Tuple2<Address, BufferObjectDataInput> inputItem = queue.poll();
            Address caller = inputItem.f0();
            BufferObjectDataInput serialized = inputItem.f1();
            if (inputItem == null) {
                return NO_PROGRESS;
            }
            try {
                long requestId = serialized.readLong();
                Object input = serialized.readObject();
                CompletableFuture future = new CompletableFuture();
                consumer.accept(input, future);
                future.whenComplete((r, t) -> networking.sendRpcResponse(caller, requestId, t == null ? r : t));
            } catch (IOException e) {
                throw rethrow(e);
            }
            return MADE_PROGRESS;
        }
    }
}
