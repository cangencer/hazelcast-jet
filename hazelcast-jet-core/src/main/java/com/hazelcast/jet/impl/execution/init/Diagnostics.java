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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicIntegerArray;

public class Diagnostics {

    public Map<String, EdgeD> edges = new HashMap<>();

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
            int sum = 0;
            int count = 0;
            for (AtomicIntegerArray conveyor : localConveyors) {
                for (int j = 0; j < conveyor.length() - (isDistributed() ? 1 : 0); j++) {
                    sum += conveyor.get(j);
                    count++;
                }
            }
            return sum / count;
        }

        public int itemsToSenders() {
            if (!isDistributed()) {
                return -1;
            }

            int sum = 0;
            int count = 0;
            for (AtomicIntegerArray senderConveyor : senderConveyors) {
                for (int i = 0; i < senderConveyor.length(); i++) {
                    sum += senderConveyor.get(i);
                    count++;
                }
            }
            return sum / count;
        }

        public int itemsFromReceiver() {
            if (!isDistributed()) {
                return -1;
            }
            int sum = 0;
            for (AtomicIntegerArray conveyor : localConveyors) {
                sum += conveyor.get(conveyor.length() - 1);
            }
            return sum / localConveyors.length;
        }

        public String name() {
            return source + "->" + destination;
        }

        private boolean isDistributed() {
            return senderConveyors != null;
        }

        @Override public String toString() {
            if (isDistributed()) {
                return source + "->" + destination + "\t\t l:" + localInFlightItems() + " s:" + itemsToSenders() + " r:" +
                        itemsFromReceiver();
            }
            return source + "->" + destination + "\t\t l:" + localInFlightItems();
        }
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (EdgeD edgeD : edges.values()) {
            builder.append(edgeD.toString()).append("\n");
        }
        return builder.toString();
    }
}
