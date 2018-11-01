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

import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.impl.execution.Tasklet;
import com.hazelcast.jet.impl.util.ProgressState;

public class EndpointContext {
    private final String name;
    private final DistributedBiConsumer consumer;

    public EndpointContext(String name, DistributedBiConsumer consumer) {
        this.name = name;
        this.consumer = consumer;
    }


    public class Worker implements Tasklet {

        public ProgressState call() {
            return ProgressState.NO_PROGRESS;
        }
    }
}
