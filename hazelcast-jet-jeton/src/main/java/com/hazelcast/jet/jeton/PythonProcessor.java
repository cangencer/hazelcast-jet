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

import com.hazelcast.jet.Inbox;
import com.hazelcast.jet.Outbox;
import com.hazelcast.jet.Processor;

import javax.annotation.Nonnull;

public class PythonProcessor implements Processor {

    private final Transform transform;

    public PythonProcessor(Transform transform) {
        this.transform = transform;
    }

    @Override
    public void init(@Nonnull Outbox outbox, @Nonnull Context context) {

    }

    @Override
    public void process(int ordinal, @Nonnull Inbox inbox) {

    }

    @Override
    public boolean complete() {
        return true;
    }
}
