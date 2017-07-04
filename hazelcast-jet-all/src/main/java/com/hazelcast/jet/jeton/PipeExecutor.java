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
                prev = dag.newVertex()
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
