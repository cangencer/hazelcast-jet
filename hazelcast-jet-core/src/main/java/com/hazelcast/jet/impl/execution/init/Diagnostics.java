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

import com.hazelcast.nio.Address;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicIntegerArray;

public class Diagnostics implements Serializable {

    public Map<String, EdgeD> edges = new HashMap<>();

    public Map<Address, Map<String, DiagData>> remoteData = new ConcurrentHashMap<>();

    public Map<String, DiagData> localData() {
        Map<String, DiagData> map = new HashMap<>();
        for (Entry<String, EdgeD> entry : edges.entrySet()) {
            map.put(entry.getKey(), entry.getValue().data());
        }
        return map;
    }

    public Map<String, DiagData> aggrData() {
        Map<String, DiagData> aggrData = new HashMap<>();
        Map<String, DiagData> localData = localData();

        for (Entry<String, DiagData> e : localData.entrySet()) {
            List<DiagData> list = new ArrayList<>();
            list.add(e.getValue());
            String edge = e.getKey();
            for (Map<String, DiagData> re : remoteData.values()) {
                list.add(re.get(edge));
            }
            aggrData.put(edge, aggregate(list));
        }
        return aggrData;
    }

    private DiagData aggregate(List<DiagData> data) {
        double localUtil = data.stream().mapToInt(d -> d.localUtilization).average().getAsDouble();
        double senderUtil = data.stream().mapToInt(d -> d.senderUtilization).average().getAsDouble();
        double receiverUtil = data.stream().mapToInt(d -> d.receiverUtilization).average().getAsDouble();
        return new DiagData((int) localUtil, (int) senderUtil, (int) receiverUtil);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (EdgeD edgeD : edges.values()) {
            builder.append(edgeD.toString()).append("\n");
        }
        return builder.toString();
    }

    public void updateRemoteData(Address fromAddr, Map<String, DiagData> data) {
        remoteData.put(fromAddr, data);
    }

    public static class EdgeD {

        final String name;
//
//        int localInFlightItems; // sum of all local conveyors queues except the receiver queue
//        int itemsToSenders; // sum of conveyors between processors and sender tasklets
//        int itemsFromReceiver; // sum of items in the receiver queue for each local conveyor


        // one AtomicIntegerArray per conveyor , each slot in a atomic array is the size of a queue
        AtomicIntegerArray[] localConveyors; // last index in each atomic array is the receiver queue size
        AtomicIntegerArray[] senderConveyors;

        EdgeD(String name) {
            this.name = name;
        }

        public int localUtilization() {
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

        public int senderUtilization() {
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

        public int receiverUtilization() {
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
            return name;
        }

        public DiagData data() {
            return new DiagData(localUtilization(), senderUtilization(), receiverUtilization());
        }

        private boolean isDistributed() {
            return senderConveyors != null;
        }

        @Override
        public String toString() {
            if (isDistributed()) {
                return name() + "\t\t l:" + localUtilization() + " s:" + senderUtilization() + " r:" +
                        receiverUtilization();
            }
            return name() + "\t\t l:" + localUtilization();
        }
    }

    public static class DiagData implements Serializable {

        public int localUtilization;
        public int senderUtilization;
        public int receiverUtilization;

        DiagData(int localUtilization, int senderUtilization, int receiverUtilization) {
            this.localUtilization = localUtilization;
            this.senderUtilization = senderUtilization;
            this.receiverUtilization = receiverUtilization;
        }

        @Override
        public String toString() {
            if (senderUtilization == -1) {
                return "local=" + localUtilization;
            }

            return "local=" + localUtilization +
                    ", sender=" + senderUtilization +
                    ", receiver=" + receiverUtilization;
        }
    }
}
