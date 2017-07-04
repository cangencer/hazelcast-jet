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

import static com.hazelcast.jet.function.DistributedFunctions.entryKey;
import static com.hazelcast.jet.stream.impl.StreamUtil.uniqueVertexName;

public class PipeExecutor {

    public static DAG buildDag(List<Transform> transformList) {
        DAG dag = new DAG();
        Vertex prev = null;
        for (Transform transform : transformList) {
            if (transform.getName().equals("readMap")) {
                String mapName = transform.getParam(0);
                if (prev != null) {
                    throw new IllegalStateException("prev vertex is not null");
                }
                prev = dag.newVertex("read-" + mapName, Sources.readMap(mapName));
            }
            else if (transform.getName().equals("filter")) {
                Vertex filter = dag.newVertex(uniqueVertexName("filter"), () -> new PythonProcessor(transform));
                dag.edge(Edge.between(prev, filter));
                prev = filter;
            }
            else if (transform.getName().equals("flatMap")) {
                Vertex flatMap = dag.newVertex(uniqueVertexName("flatMap"), () -> new PythonProcessor(transform));
                dag.edge(Edge.between(prev, flatMap));
                prev = flatMap;
            }
            else if (transform.getName().equals("map")) {
                Vertex map = dag.newVertex(uniqueVertexName("map"), () -> new PythonProcessor(transform));
                dag.edge(Edge.between(prev, map));
                prev = map;
            }
            else if (transform.getName().equals("reduce")) {
                Vertex accumulate = dag.newVertex(uniqueVertexName("accumulate"), () -> new PythonProcessor(transform));
                Vertex combine = dag.newVertex(uniqueVertexName("combine"), () -> new PythonProcessor(transform));
                dag.edge(Edge.between(prev, accumulate).partitioned(entryKey()));
                dag.edge(Edge.between(accumulate, combine).distributed().partitioned(entryKey()));
                prev = combine;
            }
            else if (transform.getName().equals("writeMap")) {
                String mapName = transform.getParam(0);
                if (prev == null) {
                    throw new IllegalStateException("prev vertex is null");
                }
                Vertex sink = dag.newVertex("write-" + mapName, Sinks.writeMap(mapName));
                dag.edge(Edge.between(prev, sink));
            }
        }
        return dag;
    }
}
