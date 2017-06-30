/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class IgniteClusterActivateDeactivateTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final String CACHE_NAME_PREFIX = "cache-";

    /** */
    private boolean client;

    /** */
    private boolean active = true;

    /** */
    private CacheConfiguration[] ccfgs;

    /** */
    private boolean testSpi;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setClientMode(client);

        cfg.setActiveOnStart(active);

        if (ccfgs != null) {
            cfg.setCacheConfiguration(ccfgs);

            ccfgs = null;
        }

        MemoryConfiguration memCfg = new MemoryConfiguration();
        memCfg.setPageSize(1024);
        memCfg.setDefaultMemoryPolicySize(10 * 1024 * 1024);

        cfg.setMemoryConfiguration(memCfg);

        if (persistenceEnabled()) {
            PersistentStoreConfiguration pCfg = new PersistentStoreConfiguration();

            pCfg.setWalMode(WALMode.LOG_ONLY);

            cfg.setPersistentStoreConfiguration(pCfg);
        }

        if (testSpi)
            cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @return {@code True} if test with persistence.
     */
    protected boolean persistenceEnabled() {
        return false;
    }

    /**
     * @throws Exception If failed.
     */
    public void testActivateSimpleSingleNode() throws Exception {
        activateSimple(1, 0, 0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testActivateSimple_5_Servers() throws Exception {
        activateSimple(5, 0, 0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testActivateSimple_5_Servers2() throws Exception {
        activateSimple(5, 0, 4);
    }

    /**
     * @throws Exception If failed.
     */
    public void testActivateSimple_5_Servers_5_Clients() throws Exception {
        activateSimple(5, 4, 0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testActivateSimple_5_Servers_5_Clients_FromClient() throws Exception {
        activateSimple(5, 4, 6);
    }

    /**
     * @param srvs Number of servers.
     * @param clients Number of clients.
     * @param activateFrom Index of node running activation.
     * @throws Exception If failed.
     */
    private void activateSimple(int srvs, int clients, int activateFrom) throws Exception {
        active = false;

        final int CACHES = 2;

        for (int i = 0; i < srvs; i++) {
            client = false;

            ccfgs = cacheConfigurations1();

            startGrid(i);
        }

        for (int i = 0; i < clients; i++) {
            client = true;

            ccfgs = cacheConfigurations1();

            startGrid(srvs + i);
        }

        ignite(activateFrom).active(false); // Should be no-op.

        ignite(activateFrom).active(true);

        checkCaches(srvs + clients, CACHES);

        client = false;

        startGrid(srvs + clients);

        checkCaches(srvs + clients + 1, CACHES);

        client = true;

        startGrid(srvs + clients + 1);

        checkCaches(srvs + clients + 2, CACHES);
    }

    /**
     * @param nodes Number of nodes.
     * @param caches Number of caches.
     */
    private void checkCaches(int nodes, int caches) {
        for (int i  = 0; i < nodes; i++) {
            for (int c = 0; c < caches; c++) {
                IgniteCache<Integer, Integer> cache = ignite(i).cache(CACHE_NAME_PREFIX + c);

                for (int j = 0; j < 10; j++) {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    Integer key = rnd.nextInt(1000);

                    cache.put(key, j);

                    assertEquals((Integer)j, cache.get(key));
                }
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testJoinWhileActivate1() throws Exception {
        active = false;
        testSpi = true;

        for (int i = 0; i < 2; i++) {
            ccfgs = cacheConfigurations1();

            startGrid(i);
        }

        TestRecordingCommunicationSpi spi1 = TestRecordingCommunicationSpi.spi(ignite(1));

        spi1.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode clusterNode, Message msg) {
                if (msg instanceof GridDhtPartitionsSingleMessage) {
                    GridDhtPartitionsSingleMessage pMsg = (GridDhtPartitionsSingleMessage)msg;

                    if (pMsg.exchangeId() != null && pMsg.exchangeId().topologyVersion().topologyVersion() == 2)
                        return true;
                }

                return false;
            }
        });

        IgniteInternalFuture<?> activeFut = GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                ignite(0).active(true);
            }
        });

        spi1.waitForBlocked();

        U.sleep(500);

        assertFalse(activeFut.isDone());

        IgniteInternalFuture<?> startFut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                ccfgs = cacheConfigurations1();

                startGrid(2);

                return null;
            }
        });

        spi1.stopBlock();

        activeFut.get();
        startFut.get();

        checkCaches(3, 2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testJoinAndActivate() throws Exception {
        for (int iter = 0; iter < 3; iter++) {
            log.info("Iteration: " + iter);

            active = false;

            for (int i = 0; i < 3; i++) {
                ccfgs = cacheConfigurations1();

                startGrid(i);
            }

            final int START_NODES = 3;

            final CyclicBarrier b = new CyclicBarrier(START_NODES + 1);

            IgniteInternalFuture<?> fut1 = GridTestUtils.runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    b.await();

                    Thread.sleep(ThreadLocalRandom.current().nextLong(100) + 1);

                    ignite(0).active(true);

                    return null;
                }
            });

            final AtomicInteger nodeIdx = new AtomicInteger(3);

            IgniteInternalFuture<?> fut2 = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    int idx = nodeIdx.getAndIncrement();

                    b.await();

                    startGrid(idx);

                    return null;
                }
            }, START_NODES, "start-node");

            fut1.get();
            fut2.get();

            checkCaches(6, 2);

            afterTest();
        }
    }

    /**
     * @return Cache configurations.
     */
    private CacheConfiguration[] cacheConfigurations1() {
        CacheConfiguration[] ccfgs = new CacheConfiguration[2];

        ccfgs[0] = cacheConfigurations(CACHE_NAME_PREFIX + 0, ATOMIC);
        ccfgs[1] = cacheConfigurations(CACHE_NAME_PREFIX + 1, TRANSACTIONAL);

        return ccfgs;
    }

    /**
     * @param name Cache name.
     * @param atomicityMode Atomicity mode.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfigurations(String name, CacheAtomicityMode atomicityMode) {
        CacheConfiguration ccfg = new CacheConfiguration(name);

        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setBackups(1);

        return ccfg;
    }
}
