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
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.Member;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static java.util.concurrent.CompletableFuture.completedFuture;

@SuppressWarnings("Duplicates")
public class EndpointBenchmark {

    public static final int PENDING_LIMIT = 1_000;
    private JetInstance instance;
    private JetInstance liteMember;

    @Before
    public void setup() {
        JetConfig memberConfig = new JetConfig();
//        memberConfig.getInstanceConfig().setCooperativeThreadCount(1);
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

        instance.<Tuple2<Integer, Integer>, Integer>newEndpoint("sum", t -> completedFuture(t.f0() + t.f1()));

    }

    @Test
    public void demo() throws InterruptedException {
        instance.newEndpoint("sum",
                (Tuple2<Integer, Integer> integers) -> completedFuture(integers.f0() + integers.f1())
        );

        IEndpoint<Tuple2<Integer, Integer>, Integer> endpoint = liteMember.getEndpoint("sum");

        Integer response = endpoint.call(tuple2(10, 20));

        System.out.println("Response: " + response);
    }

//    @Test
//    public void demoWithContext() throws InterruptedException {
//        instance.<HttpClient, Tuple2<Integer, Integer>, Integer>newEndpoint("lookup", httpClient(),
//                (c, t) -> c.sendAsync(GET(".."), responseInfo -> null).handle((r, th) -> 0));
//    }
//
//    private static HttpRequest GET(String uri) {
//        return HttpRequest.newBuilder().uri(URI.create(uri)).build();
//    }
//
//    private static ContextFactory<HttpClient> httpClient() {
//        return ContextFactory
//                .withCreateFn(jet -> HttpClient.newHttpClient())
//                .shareLocally();
//    }


    @Test
    public void testSync() {
        IEndpoint<Tuple2<Integer, Integer>, Integer> endpoint = liteMember.getEndpoint("sum");
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
        IEndpoint<Tuple2<Integer, Integer>, Integer> endpoint = liteMember.getEndpoint("sum");
        ForkJoinPool.commonPool().execute(() -> {
            while (true) {
                if (pendingCount.get() == PENDING_LIMIT) {
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


    @Test
    public void testExecutorSync() throws ExecutionException, InterruptedException {
        long start = System.nanoTime();
        int interval = 5_000;
        int i = 0;
        IExecutorService executor = liteMember.getHazelcastInstance().getExecutorService("executor");
        while(true) {
            if (i++ % interval == 0) {
                double elapsed = (double) TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                double reqPerSec = interval / elapsed * 1000;
                double msPerReq = elapsed / interval;
                System.out.printf("%d: %,.3f req/sec %.3fms per request\n", i, reqPerSec, msPerReq);
                start = System.nanoTime();
            }
            Member remoteMember = instance.getHazelcastInstance().getCluster().getLocalMember();
            executor.submitToMember(new Sum(tuple2(10, 20)), remoteMember).get();
        }
    }

    @Test
    public void testExecutor() throws InterruptedException {
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger pendingCount = new AtomicInteger(0);

        IExecutorService executor = liteMember.getHazelcastInstance().getExecutorService("executor");
        long start = System.nanoTime();
        System.out.println("Starting benchmark");

        ExecutionCallback callback = new ExecutionCallback<Integer>() {
            @Override
            public void onResponse(Integer response) {
                successCount.incrementAndGet();
                pendingCount.decrementAndGet();
            }

            @Override
            public void onFailure(Throwable t) {

            }
        };
        ForkJoinPool.commonPool().execute(() -> {
            while (true) {
                if (pendingCount.get() == PENDING_LIMIT) {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                    continue;
                }
                Member remoteMember = instance.getHazelcastInstance().getCluster().getLocalMember();
                executor.submitToMember(new Sum(tuple2(10, 20)), remoteMember, callback);
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

    static class Sum implements Callable<Integer>, DataSerializable {
        private Tuple2<Integer, Integer> request;

        public Sum() {
        }

        public Sum(Tuple2<Integer, Integer> request) {
            this.request = request;
        }


        @Override
        public Integer call() throws Exception {
            return request.f1() + request.f0();
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(request);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            request = in.readObject();
        }
    }
}
