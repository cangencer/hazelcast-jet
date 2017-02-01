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

package com.hazelcast.jet.impl.execution.init;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicIntegerArray;

public class Diagnostics {

    Map<String, EdgeD> edges = new HashMap<>();

    public static class EdgeD {

        final String id;
        final String source;
        final String destination;
//
//        int localInFlightItems; // sum of all local conveyors queues except the receiver queue
//        int itemsToSenders; // sum of conveyors between processors and sender tasklets
//        int itemsFromReceiver; // sum of items in the receiver queue for each local conveyor


        // one AtomicIntegerArray per conveyor , each slot in a atomic array is the size of a queue
        AtomicIntegerArray[] localConveyors; // last index in each atomic array is the receiver queue size
        AtomicIntegerArray[] senderConveyors;

        EdgeD(String id, String source, String destination) {
            this.id = id;
            this.source = source;
            this.destination = destination;
        }

        public int localInFlightItems() {
            return 0;
        }

        public int itemsToSenders() {
            return 0;
        }

        public int itemsFromReceiver() {
            return 0;
        }

        @Override public String toString() {
            return "EdgeD{" +
                    "id='" + id + '\'' +
                    ", source='" + source + '\'' +
                    ", destination='" + destination + '\'' +
                    ", localConveyors=" + Arrays.toString(localConveyors) +
                    ", senderConveyors=" + Arrays.toString(senderConveyors) +
                    '}';
        }
    }

    @Override public String toString() {
        return edges.toString();
    }
}
