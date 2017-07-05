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
import com.hazelcast.jet.Inbox;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Outbox;
import com.hazelcast.jet.Processor;
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.BufferObjectDataOutput;

import javax.annotation.Nonnull;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

public class PythonProcessor implements Processor {

    static final String SYSPROP_PYTHON_PATH = "jet.python.path";

    private final Transform transform;
    private DataInputStream in;
    private DataOutputStream out;
    private InternalSerializationService ser = new DefaultSerializationServiceBuilder().build();
    private final ArrayDeque<Object> preOutbox = new ArrayDeque<>();
    private Outbox outbox;
    private Process pythonProcess;
    private boolean isCompleting;

    PythonProcessor(Transform transform) {
        this.transform = transform;
    }

    @Override
    public void init(@Nonnull Outbox outbox, @Nonnull Context context) {
        this.outbox = outbox;
        try {
            pythonProcess = new ProcessBuilder().command("python", "-u", System.getProperty(SYSPROP_PYTHON_PATH))
                                                .redirectError(Redirect.INHERIT)
                                                .start();
            in = new DataInputStream(pythonProcess.getInputStream());
            out = new DataOutputStream(pythonProcess.getOutputStream());
            BufferObjectDataOutput bout = ser.createObjectDataOutput();
            bout.writeObject(transform);
            byte[] packet = bout.toByteArray();
            out.writeInt(packet.length);
            out.write(packet);
            out.flush();
        } catch (IOException e) {
            throw new JetException("Python worker failed to start", e);
        }
    }

    @Override
    public void process(int ordinal, @Nonnull Inbox inbox) {
        if (!drainPython()) {
            return;
        }
        List<Object> inboxItems = new ArrayList<>();
        inbox.drainTo(inboxItems);
        sendInboxToPython(inboxItems);
    }

    @Override
    public boolean complete() {
        if (!isCompleting) {
            if (!drainPython()) {
                return false;
            }
            sendInboxToPython(new ArrayList<>());
            isCompleting = true;
        }
        if (drainPython()) {
            pythonProcess.destroy();
            return true;
        } else {
            return false;
        }
    }

    private void sendInboxToPython(List<Object> inboxItems) {
        BufferObjectDataOutput bout = ser.createObjectDataOutput();
        try {
            bout.writeObject(inboxItems);
            out.write(bout.toByteArray());
        } catch (IOException e) {
            throw new JetException("Failed to send input items to python", e);
        }
    }

    private boolean drainPython() {
        return flushPreOutbox() && !fillPreOutbox();
    }

    private boolean fillPreOutbox() {
        try {
            byte[] buf = new byte[in.readInt()];
            in.readFully(buf);
            BufferObjectDataInput bufin = ser.createObjectDataInput(buf);
            int outboxItemCount = bufin.readInt();
            for (int i = 0; i < outboxItemCount; i++) {
                preOutbox.add(bufin.readObject());
            }
            return outboxItemCount > 0;
        } catch (IOException e) {
            throw new JetException("Failed to receive outbox items from python", e);
        }
    }

    private boolean flushPreOutbox() {
        for (Object outItem; (outItem = preOutbox.peek()) != null;) {
            if (outbox.offer(outItem)) {
                preOutbox.remove();
            } else {
                return false;
            }
        }
        return true;
    }
}
