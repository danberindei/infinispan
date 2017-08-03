package org.infinispan.distribution.rehash;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.infinispan.Cache;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.configuration.cache.BiasAcquisition;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.util.concurrent.locks.LockManager;
import org.testng.annotations.Test;

/**
 * Tests data loss during state transfer when the originator of a put operation becomes the primary owner of the
 * modified key. See https://issues.jboss.org/browse/ISPN-3357
 *
 * @author Dan Berindei
 */
@Test(groups = "functional", testName = "distribution.rehash.NonTxPutIfAbsentDuringJoinStressTest")
@CleanupAfterMethod
public class NonTxPutIfAbsentDuringRebalanceStressTest extends MultipleCacheManagersTest {

   private static final int NUM_WRITERS = 4;
   private static final int NUM_ORIGINATORS = 2;
   private static final int MAX_KEYS = 200;

   @Override
   public Object[] factory() {
      return new Object[]{
         new NonTxPutIfAbsentDuringRebalanceStressTest().cacheMode(CacheMode.DIST_SYNC),
         new NonTxPutIfAbsentDuringRebalanceStressTest().cacheMode(CacheMode.SCATTERED_SYNC)
            .biasAcquisition(BiasAcquisition.NEVER),
         new NonTxPutIfAbsentDuringRebalanceStressTest().cacheMode(CacheMode.SCATTERED_SYNC)
            .biasAcquisition(BiasAcquisition.ON_WRITE),
         };
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder c = getConfigurationBuilder();

      addClusterEnabledCacheManager(c);
      addClusterEnabledCacheManager(c);
      waitForClusterToForm();
   }

   private ConfigurationBuilder getConfigurationBuilder() {
      return getDefaultClusteredCacheConfig(cacheMode, false);
   }

   @Test(invocationCount = 10)
   public void testPutIfAbsentDuringJoin() throws Exception {
      testPutIfAbsentDuringRebalance(
         () -> {
         },
         () -> {
            addClusterEnabledCacheManager(getConfigurationBuilder());
            waitForClusterToForm();

            addClusterEnabledCacheManager(getConfigurationBuilder());
            waitForClusterToForm();
         });
   }

   @Test(invocationCount = 10)
   public void testPutIfAbsentDuringLeave() throws Exception {
      testPutIfAbsentDuringRebalance(
         () -> {
            addClusterEnabledCacheManager(getConfigurationBuilder());
            addClusterEnabledCacheManager(getConfigurationBuilder());
            addClusterEnabledCacheManager(getConfigurationBuilder());
            waitForClusterToForm();
         }, () -> {
            killMember(4);
            TestingUtil.waitForNoRebalance(caches());

            killMember(3);
            TestingUtil.waitForNoRebalance(caches());
         }
      );
   }

   private void testPutIfAbsentDuringRebalance(Runnable prepare, Runnable rebalance) throws Exception {
      prepare.run();

      ConcurrentMap<String, String> insertedValues = CollectionFactory.makeConcurrentMap();
      AtomicBoolean outerStop = new AtomicBoolean(false);
      AtomicBoolean innerStop = new AtomicBoolean(false);
      CyclicBarrier barrier = new CyclicBarrier(NUM_WRITERS, () -> innerStop.set(outerStop.get()));

      Future[] futures = new Future[NUM_WRITERS];
      for (int i = 0; i < NUM_WRITERS; i++) {
         final int writerIndex = i;
         futures[i] = fork(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
               for (int j = 0; j < MAX_KEYS; j++) {
                  // innerStop is only set in the barrier action, so all threads will see the change
                  // in the same loop iteration
                  if (innerStop.get())
                     break;

                  log.tracef("Starting loop %d", j);
                  try {
                     Cache<Object, Object> cache = cache(writerIndex % NUM_ORIGINATORS);
                     doPut(cache, "key_" + j, "value_" + j + "_" + writerIndex);
                  } catch (Throwable t) {
                     outerStop.set(true);
                     throw t;
                  } finally {
                     barrier.await(30, TimeUnit.SECONDS);
                  }
               }
               return null;
            }

            private void doPut(Cache<Object, Object> cache, String key, String value) throws Exception {
               Object oldValue = cache.putIfAbsent(key, value);
               Object newValue = cache.get(key);
               if (oldValue == null) {
                  // succeeded
                  log.tracef("Successfully inserted value %s for key %s", value, key);
                  assertEquals(value, newValue);
                  String duplicateInsertedValue = insertedValues.putIfAbsent(key, value);
                  if (duplicateInsertedValue != null) {
                     // ISPN-4286: two concurrent putIfAbsent operations can both return null
                     assertEquals(value, duplicateInsertedValue);
                  }
               } else {
                  log.tracef("Failed to insert value %s for key %s, putIfAbsent returned %s, get returned %s",
                             value, key, oldValue, newValue);
                  // ISPN-3918: cache.get(key) could return a different value until the successful command finishes
                  if (!oldValue.equals(newValue)) {
                     // Wait for the successful putIfAbsent to finish
                     eventually(() -> insertedValues.containsKey(key));
                     log.errorf("ISPN-3918: Inconsistency detected. putIfAbsent(%s, %s) = %s, " +
                                   "cache.get(%s) = %s, and the value in the cache should be %s",
                                key, value, oldValue, key, newValue, insertedValues.get(key));
                  }
               }
            }
         });
      }

      rebalance.run();

      outerStop.set(true);

      for (int i = 0; i < NUM_WRITERS; i++) {
         futures[i].get(60, TimeUnit.SECONDS);
      }

      for (int i = 0; i < caches().size(); i++) {
         LockManager lockManager = advancedCache(i).getLockManager();
         assertEquals(0, lockManager.getNumberOfLocksHeld());

         for (Map.Entry<String, String> e : insertedValues.entrySet()) {
            String key = e.getKey();
            assertEquals(e.getValue(), cache(i).get(key));
            assertFalse(lockManager.isLocked(key));
         }
      }
   }
}
