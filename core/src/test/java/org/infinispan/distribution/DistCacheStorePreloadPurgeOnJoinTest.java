/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.distribution;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStoreConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

/**
 * Test preloading with a distributed cache and purgeOnJoin enabled.
 *
 * @author Dan Berindei
 * @since 5.2
 */
@Test(groups = "functional", testName = "distribution.DistCacheStorePreloadTest")
public class DistCacheStorePreloadPurgeOnJoinTest extends BaseDistCacheStoreTest {

   public static final int NUM_KEYS = 10;
   public static final int NUM_OWNERS = 2;

   public DistCacheStorePreloadPurgeOnJoinTest() {
      INIT_CLUSTER_SIZE = 2;
      sync = true;
      tx = false;
      testRetVals = true;
      shared = false;
      preload = true;
      performRehashing = true;
   }

   @AfterMethod(alwaysRun = true)
   public void clearStats() {
      for (Cache<?, ?> c: caches) {
         System.out.println("Clearing stats for cache store on cache "+ c);
         DummyInMemoryCacheStore cs = (DummyInMemoryCacheStore) TestingUtil.extractComponent(c, CacheLoaderManager.class).getCacheStore();
         cs.clear();
         cs.clearStats();
      }
   }

   @Override
   protected ConfigurationBuilder buildConfiguration() {
      ConfigurationBuilder cfg = super.buildConfiguration();
      cfg.clustering().stateTransfer().purgeOnJoin(true);
      return cfg;
   }

   public void testPurgeOnJoin() throws CacheLoaderException {
      for (int i = 0; i < NUM_KEYS; i++) {
         c1.put("k" + i, "v" + i);
      }

      DataContainer dc1 = c1.getAdvancedCache().getDataContainer();
      assertEquals(NUM_KEYS, dc1.size());
      DataContainer dc2 = c2.getAdvancedCache().getDataContainer();
      assertEquals(NUM_KEYS, dc2.size());

      DummyInMemoryCacheStore cs1 = (DummyInMemoryCacheStore) TestingUtil.extractComponent(c1, CacheLoaderManager.class).getCacheStore();
      assertEquals(NUM_KEYS, cs1.size());

      DummyInMemoryCacheStore cs2 = (DummyInMemoryCacheStore) TestingUtil.extractComponent(c2, CacheLoaderManager.class).getCacheStore();
      assertEquals(NUM_KEYS, cs2.size());

      assertEquals(0, (int) cs1.stats().get("clear"));
      assertEquals(1, (int) cs2.stats().get("clear"));

      addClusterEnabledCacheManager();
      EmbeddedCacheManager cm3 = cacheManagers.get(2);
      ConfigurationBuilder cfg = buildConfiguration();
      cfg.loaders().clearCacheLoaders();
      cfg.loaders().addStore(new DummyInMemoryCacheStoreConfigurationBuilder(cfg.loaders())
            .storeName(getClass().getSimpleName()));
      cm3.defineConfiguration(cacheName, cfg.build());
      c3 = cm3.getCache(cacheName);
      waitForClusterToForm();

      DataContainer dc3 = c3.getAdvancedCache().getDataContainer();
      int cache3Size = dc3.size();
      assertTrue("Expected some of the cache store entries to be transferred to the third cache", cache3Size > 0);
      assertEquals("Expected " + (2 * NUM_KEYS) + " entries, got: " + Arrays.asList(dc1.keySet(), dc2.keySet(), dc3.keySet()),
            NUM_OWNERS * NUM_KEYS, dc1.size() + dc2.size() + dc3.size());

      DummyInMemoryCacheStore cs3 = (DummyInMemoryCacheStore) TestingUtil.extractComponent(c3, CacheLoaderManager.class).getCacheStore();
      assertEquals(cache3Size, cs3.size());

      assertEquals(0, (int) cs1.stats().get("clear"));
      assertEquals(1, (int) cs2.stats().get("clear"));
      assertEquals(1, (int) cs3.stats().get("clear"));

      // Remove all the keys while cache 2 is stopped
      c3.stop();

      // Needed because of ISPN-3443, otherwise state transfer can undo the removals
      TestingUtil.waitForRehashToComplete(c1, c2);

      for (int i = 0; i < NUM_KEYS; i++) {
         c1.remove("k" + i);
      }
      assertEquals("Expected 0 entries, got " + dc1.keySet(), 0, dc1.size());
      assertEquals("Expected 0 entries, got " + cs1.loadAllKeys(null), 0, cs1.size());
      assertEquals("Expected 0 entries, got " + cs2.loadAllKeys(null), 0, cs2.size());
      assertEquals(cache3Size, cs3.size());
      
      // Start cache 2 back up and check that it doesn't contain any keys
      c3.start();

      dc3 = c3.getAdvancedCache().getDataContainer();
      assertEquals("Expected 0 entries, got " + dc3.keySet(), 0, dc3.size());
      
      cs3 = (DummyInMemoryCacheStore) TestingUtil.extractComponent(c3, CacheLoaderManager.class).getCacheStore();
      assertEquals("Expected 0 entries, got " + cs3.loadAllKeys(null), 0, cs3.size());

      assertEquals(0, (int) cs1.stats().get("clear"));
      assertEquals(1, (int) cs2.stats().get("clear"));
      assertEquals(2, (int) cs3.stats().get("clear"));
   }
}
