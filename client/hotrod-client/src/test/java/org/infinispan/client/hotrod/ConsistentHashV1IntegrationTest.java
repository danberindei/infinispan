/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.infinispan.client.hotrod;

import org.infinispan.affinity.KeyAffinityService;
import org.infinispan.affinity.KeyAffinityServiceFactory;
import org.infinispan.client.hotrod.impl.RemoteCacheImpl;
import org.infinispan.client.hotrod.impl.transport.tcp.TcpTransportFactory;
import org.infinispan.client.hotrod.retry.DistributionRetryTest;
import org.infinispan.config.Configuration;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.remoting.transport.Address;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.util.ReflectionUtil;
import org.infinispan.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.*;

/**
 * @author Mircea Markus
 */
@Test (groups = "functional", testName = "client.hotrod.ConsistentHashV1IntegrationTest")
public class ConsistentHashV1IntegrationTest extends MultipleCacheManagersTest {

   private HotRodServer hotRodServer1;
   private HotRodServer hotRodServer2;
   private HotRodServer hotRodServer3;
   private HotRodServer hotRodServer4; //tod add shutdown behaviour
   private RemoteCacheManager remoteCacheManager;
   private RemoteCacheImpl remoteCache;
   private KeyAffinityService kas;
   private ExecutorService ex;

   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration conf = getDefaultClusteredConfig(Configuration.CacheMode.DIST_SYNC, false);
      conf.fluent().jmxStatistics();
      assert conf.isExposeJmxStatistics();
      conf.fluent().hash().numOwners(2);
      conf.fluent().hash().rehashEnabled(false);

      addClusterEnabledCacheManager(conf);
      addClusterEnabledCacheManager(conf);
      addClusterEnabledCacheManager(conf);
      addClusterEnabledCacheManager(conf);

      hotRodServer1 = TestHelper.startHotRodServer(manager(0));
      hotRodServer2 = TestHelper.startHotRodServer(manager(1));
      hotRodServer3 = TestHelper.startHotRodServer(manager(2));
      hotRodServer4 = TestHelper.startHotRodServer(manager(3));


      waitForClusterToForm();

      Properties clientConfig = new Properties();
      clientConfig.put("infinispan.client.hotrod.server_list", "localhost:" + hotRodServer2.getPort());

      remoteCacheManager = new RemoteCacheManager(clientConfig);
      remoteCache = (RemoteCacheImpl) remoteCacheManager.getCache();
      assert super.cacheManagers.size() == 4;

      ex = Executors.newSingleThreadExecutor();
      kas = KeyAffinityServiceFactory.newKeyAffinityService(cache(0),
                                                            ex,
                                                            new DistributionRetryTest.ByteKeyGenerator(),
                                                            2, true);

      for (int i = 0; i < 4; i++) {
         advancedCache(i).addInterceptor(new HitsAwareCacheManagersTest.HitCountInterceptor(), 1);
      }
   }

   @AfterMethod(alwaysRun = true)
   @Override
   protected void clearContent() throws Throwable {
   }

   @AfterTest(alwaysRun = true)
   public void cleanUp() {
      ex.shutdownNow();
      kas.stop();

      stopServer(hotRodServer1);
      stopServer(hotRodServer2);
      stopServer(hotRodServer3);
      stopServer(hotRodServer4);

      remoteCache.stop();
      remoteCacheManager.stop();
   }

   private void stopServer(HotRodServer hrs) {
      killServers(hrs);
   }

   public void testCorrectBalancingOfKeys() {
      runTest(0);
      runTest(1);
      runTest(2);
      runTest(3);
   }

   private void runTest(int cacheIndex) {
      ConsistentHash serverCH = advancedCache(cacheIndex).getDistributionManager().getConsistentHash();

      // compatibility with 1.0/1.1 clients is not perfect, so we must allow for some misses
      int misses = 0;
      for (int i = 0; i < 1000; i++) {
         byte[] keyBytes = (byte[]) kas.getKeyForAddress(address(cacheIndex));
         String key = DistributionRetryTest.ByteKeyGenerator.getStringObject(keyBytes);
         List<Address> serverBackups = serverCH.locateOwners(keyBytes);
         assert serverBackups.contains(address(cacheIndex));
         remoteCache.put(key, "v");

         Address hitServer = getHitServer();
         if (!serverBackups.contains(hitServer)) {
            misses++;
         }

         assert misses < 10 : String.format("i=%s, backups: %s, hit server: %s, key=%s", i, serverBackups, hitServer, Util.printArray(key.getBytes(), false));
      }

   }

   private Address getHitServer() {
      List<Address> result = new ArrayList<Address>();
      for (int i = 0; i < 4; i++) {
         InterceptorChain ic = advancedCache(i).getComponentRegistry().getComponent(InterceptorChain.class);
         HitsAwareCacheManagersTest.HitCountInterceptor interceptor =
               (HitsAwareCacheManagersTest.HitCountInterceptor) ic.getInterceptorsWithClass(HitsAwareCacheManagersTest.HitCountInterceptor.class).get(0);
         if (interceptor.getHits() == 1) {
            result.add(address(i));
         }
         interceptor.reset();
      }
      if (result.size() > 1) throw new IllegalStateException("More than one hit! : " + result);
      return result.get(0);
   }
}
