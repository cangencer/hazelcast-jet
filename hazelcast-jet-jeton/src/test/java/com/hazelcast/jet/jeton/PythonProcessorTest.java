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

import com.hazelcast.jet.test.TestSupport;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.jet.jeton.PythonProcessor.SYSPROP_PYTHON_PATH;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

/**
 * Javadoc pending.
 */
public class PythonProcessorTest {

    @Test
    public void test() {
        List<Object> list = new ArrayList<>();
        list.add("a");
        list.add("b");
        list.addAll(IntStream.range(0, 2000).boxed().collect(Collectors.toList()));
        System.setProperty(SYSPROP_PYTHON_PATH, "/Users/can/src/jeton/jeton/worker.py");
        TestSupport.testProcessor(() -> new PythonProcessor(new Transform("test", new ArrayList<>())),
                asList("a", "b"), list, false);
    }
}
