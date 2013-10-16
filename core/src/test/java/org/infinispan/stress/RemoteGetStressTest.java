package org.infinispan.stress;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jgroups.protocols.DISCARD;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * Test what happens when we run a lot of remote gets from the same node in parallel.
 *
 * @author Dan Berindei
 * @since 6.0
 */
@Test(groups = "stress" , testName="stress.RemoteGetStressTest")
public class RemoteGetStressTest extends MultipleCacheManagersTest {
   private static final Log log = LogFactory.getLog(RemoteGetStressTest.class);
   public static final int NUM_KEYS = 500;
   public static final int BIG_OBJECT_SIZE = 100000;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      builder.clustering().cacheMode(CacheMode.DIST_SYNC)
            .clustering().sync().replTimeout(60, SECONDS)
            .l1().disable()
            .clustering().stateTransfer().fetchInMemoryState(true)
            .clustering().hash().numOwners(3);
      createCluster(builder, 4);

      waitForClusterToForm();
   }

   public void testDelayedRemoteGetResponses() throws Exception {
      Map<Integer, BigObject> entries = new HashMap<Integer, BigObject>();
      final Cache<Integer, BigObject> c0 = cache(0);
      Address a0 = address(0);
      ConsistentHash ch = c0.getAdvancedCache().getDistributionManager().getReadConsistentHash();

      // Find NUM_KEYS keys that are not owned by cache0
      for (int i = 0; i < NUM_KEYS; ) {
         BigObject bigObject = new BigObject(i);
         if (!ch.isKeyLocalToNode(a0, i)) {
            entries.put(i, bigObject);
            c0.put(i, bigObject);
         }
         i++;
      }

      DISCARD discard = TestingUtil.getDiscardForCache(c0);
      discard.setUpDiscardRate(0.2);

      // Now try to read all the keys from cache0 in parallel
      log.debugf("%d entries inserted, now trying to read them in parallel", NUM_KEYS);
      List<Future<BigObject>> futures = new ArrayList<Future<BigObject>>(NUM_KEYS);
      for (final Integer key : entries.keySet()) {
         futures.add(fork(new Callable<BigObject>() {
            @Override
            public BigObject call() throws Exception {
               return c0.get(key);
            }
         }));
      }

      log.debugf("Waiting for all the get operations to finish");
      for (Future<BigObject> f : futures) {
         BigObject value = f.get(10, SECONDS);
         assertNotNull(value);
      }
   }

   private static class BigObject implements Serializable {
      private static final long serialVersionUID = -5113933343337107784L;

      int num;

      public BigObject(int num) {
         this.num = num;
      }

      public BigObject() {
      }

      private void writeObject(ObjectOutputStream out) throws IOException {
         out.defaultWriteObject();
         byte[] buf = new byte[BIG_OBJECT_SIZE];
         out.write(buf);
      }

      private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
         in.defaultReadObject();
         byte[] buf = new byte[BIG_OBJECT_SIZE];
         in.read(buf);
         TestingUtil.sleepRandom(10);
      }

      @Override
      public String toString() {
         return "BigObject#" + num;
      }
   }
}
