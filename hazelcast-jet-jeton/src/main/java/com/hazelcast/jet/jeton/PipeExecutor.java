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

import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Edge;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.processor.Sinks;
import com.hazelcast.jet.processor.Sources;

import java.util.List;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.function.DistributedFunctions.entryKey;
import static com.hazelcast.jet.stream.impl.StreamUtil.uniqueVertexName;

public class PipeExecutor {

    public static DAG buildDag(List<Transform> transformList) {
        DAG dag = new DAG();
        Vertex prev = null;
        for (Transform transform : transformList) {
            switch (transform.getName()) {
                case "read_map": {
                    String mapName = transform.getParam(0);
                    if (prev != null) {
                        throw new IllegalStateException("prev vertex is not null");
                    }
                    prev = dag.newVertex("read-" + mapName, Sources.readMap(mapName));
                    break;
                }
                case "filter":
                    Vertex filter = dag.newVertex(uniqueVertexName("filter"), () -> new PythonProcessor(transform));
                    dag.edge(between(prev, filter));
                    prev = filter;
                    break;
                case "flat_map":
                    Vertex flatMap = dag.newVertex(uniqueVertexName("flatMap"), () -> new PythonProcessor(transform));
                    dag.edge(between(prev, flatMap));
                    prev = flatMap;
                    break;
                case "map":
                    Vertex map = dag.newVertex(uniqueVertexName("map"), () -> new PythonProcessor(transform));
                    dag.edge(between(prev, map));
                    prev = map;
                    break;
                case "reduce":
                    Vertex accumulate = dag.newVertex(uniqueVertexName("accumulate"), () -> new PythonProcessor(transform));
                    Vertex combine = dag.newVertex(uniqueVertexName("combine"), () -> new PythonProcessor(transform));
                    dag.edge(between(prev, accumulate).partitioned(entryKey()));
                    dag.edge(between(accumulate, combine).distributed().partitioned(entryKey()));
                    prev = combine;
                    break;
                case "write_map": {
                    String mapName = transform.getParam(0);
                    if (prev == null) {
                        throw new IllegalStateException("prev vertex is null");
                    }
                    Vertex sink = dag.newVertex("write-" + mapName, Sinks.writeMap(mapName));
                    dag.edge(between(prev, sink));
                    break;
                }
            }
        }
        return dag;
    }
}
