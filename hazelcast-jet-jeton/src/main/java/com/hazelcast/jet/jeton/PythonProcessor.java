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

import com.hazelcast.jet.AbstractProcessor;

import javax.annotation.Nonnull;

public class PythonProcessor extends AbstractProcessor {

    private final Transform transform;

    public PythonProcessor(Transform transform) {
        this.transform = transform;
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        // init processor
        super.init(context);
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
        return true;
    }

    @Override
    public boolean complete() {
        return true;
    }
}
