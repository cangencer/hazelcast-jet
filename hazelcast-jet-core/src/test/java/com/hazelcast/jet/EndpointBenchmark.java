/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet;

import com.hazelcast.config.NetworkConfig;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.datamodel.Tuple2;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;

@SuppressWarnings("Duplicates")
public class EndpointBenchmark {

    private JetInstance instance;
    private JetInstance liteMember;
    private IEndpoint<Tuple2<Integer, Integer>, Integer> endpoint;

    @Before
    public void setup() {
        JetConfig memberConfig = new JetConfig();
        NetworkConfig nwConfig = memberConfig.getHazelcastConfig().getNetworkConfig();
        nwConfig.getJoin().getMulticastConfig().setEnabled(false);
        nwConfig.getJoin().getTcpIpConfig().setEnabled(true);
        nwConfig.getJoin().getTcpIpConfig().addMember("127.0.0.1");
        memberConfig.getHazelcastConfig().setNetworkConfig(nwConfig);
        instance = Jet.newJetInstance(memberConfig);

        JetConfig liteMemberConfig = new JetConfig();
        liteMemberConfig.getHazelcastConfig().setLiteMember(true);
        liteMemberConfig.getHazelcastConfig().setNetworkConfig(nwConfig);
        liteMember = Jet.newJetInstance(liteMemberConfig);

        instance.<Tuple2<Integer, Integer>, Integer>newEndpoint("sum", (t, f) -> {
            f.complete(t.f0() + t.f1());
        });

        endpoint = liteMember.getEndpoint("sum");
    }

    @Test
    public void demo() throws InterruptedException {
        endpoint = liteMember.getEndpoint("sum");

        Integer response = endpoint.call(tuple2(10, 20));

        System.out.println("Response: " + response);
    }

    @Test
    public void testSync() {
        long start = System.nanoTime();
        int interval = 5_000;
        int i = 0;
        while(true) {
            if (i++ % interval == 0) {
                double elapsed = (double) TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                double reqPerSec = interval / elapsed * 1000;
                double msPerReq = elapsed / interval;
                System.out.printf("%d: %,.3f req/sec %.3fms per request\n", i, reqPerSec, msPerReq);
                start = System.nanoTime();
            }
            endpoint.call(tuple2(10, 20));
        }
    }

    @Test
    public void testAsync() throws InterruptedException {
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger pendingCount = new AtomicInteger(0);

        long start = System.nanoTime();
        System.out.println("Starting benchmark");
        ForkJoinPool.commonPool().execute(() -> {
            while (true) {
                if (pendingCount.get() == 1_000) {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                    continue;
                }
                endpoint.callAsync(tuple2(10, 20)).whenComplete((r, t) -> {
                    successCount.incrementAndGet();
                    pendingCount.decrementAndGet();
                });
                pendingCount.incrementAndGet();
            }
        });

        while (true) {
            Thread.sleep(2_000);
            int count = successCount.get();
            double elapsed = (double) TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            double reqPerSec = count / elapsed * 1000;
            System.out.printf("%d: %,.3f req/sec\n", count, reqPerSec);
        }
    }
}
