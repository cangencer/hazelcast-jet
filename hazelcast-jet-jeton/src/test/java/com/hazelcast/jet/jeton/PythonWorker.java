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

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.BufferObjectDataOutput;

import java.io.InputStream;
import java.io.OutputStream;
import java.lang.ProcessBuilder.Redirect;

public class PythonWorker {

    public static void main(String[] args) throws Exception {
        Process p = new ProcessBuilder().command("python", "-u", "/Users/can/src/jeton/jeton/worker.py")
                                             .redirectError(Redirect.INHERIT)
                                             .start();
        InputStream in = p.getInputStream();
        OutputStream out = p.getOutputStream();

        InternalSerializationService service = new DefaultSerializationServiceBuilder().build();

        BufferObjectDataOutput output = service.createObjectDataOutput();
        output.writeObject("test");


        byte[] bytes = output.toByteArray();
        out.write(bytes);

        out.flush();
        Thread.currentThread().join();
    }
}
