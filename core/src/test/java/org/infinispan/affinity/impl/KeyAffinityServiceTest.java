package org.infinispan.affinity.impl;

import static org.testng.AssertJUnit.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.affinity.KeyAffinityServiceFactory;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.ExceptionRunnable;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(groups = "functional", testName = "affinity.KeyAffinityServiceTest")
public class KeyAffinityServiceTest extends BaseKeyAffinityServiceTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      super.INIT_CLUSTER_SIZE = 2;
      super.createCacheManagers();
      assertEquals(2, topology(caches.get(0).getCacheManager()).size());
      assertEquals(2, topology(caches.get(1).getCacheManager()).size());

      cache(0, cacheName).put("k", "v");
      assertEquals("v", cache(0, cacheName).get("k"));
      assertEquals("v", cache(1, cacheName).get("k"));


      keyAffinityService = (KeyAffinityServiceImpl<Object>) KeyAffinityServiceFactory.newKeyAffinityService(manager(0).getCache(cacheName),
            executor, new RndKeyGenerator(), 100);
   }

   public void testKeysAreCorrectlyCreated() throws Exception {
      assertEventualFullCapacity();
      assertKeyAffinityCorrectness();
   }

   @Test (dependsOnMethods = "testKeysAreCorrectlyCreated")
   public void testConcurrentConsumptionOfKeys() throws Exception {
      List<Future<Void>> consumers = new ArrayList<>();
      int keysToConsume = 1000;
      CountDownLatch consumersStart = new CountDownLatch(1);
      for (int i = 0; i < 10; i++) {
         consumers.add(fork(new KeyConsumer(keysToConsume, consumersStart)));
      }
      consumersStart.countDown();

      for (Future<Void> f : consumers) {
         f.get(10, TimeUnit.SECONDS);
      }

      assertCorrectCapacity();
   }

   @Test (dependsOnMethods = "testConcurrentConsumptionOfKeys")
   public void testServerAdded() throws InterruptedException {
      EmbeddedCacheManager cm = addClusterEnabledCacheManager();
      cm.defineConfiguration(cacheName, configuration.build());
      Cache<Object, String> cache = cm.getCache(cacheName);
      caches.add(cache);
      waitForClusterToResize();
      eventuallyEquals(3, () -> keyAffinityService.getAddress2KeysMapping().keySet().size());
      assertEventualFullCapacity();
      assertKeyAffinityCorrectness();
   }

   @Test(dependsOnMethods = "testServerAdded")
   public void testServersDropped() throws InterruptedException {
      caches.get(2).getCacheManager().stop();
      caches.remove(2);
      waitForClusterToResize();
      eventuallyEquals(2, () -> keyAffinityService.getAddress2KeysMapping().keySet().size());
      assertEventualFullCapacity();
      assertKeyAffinityCorrectness();
   }

   @Test (dependsOnMethods = "testServersDropped")
   public void testCollocatedKey() {
      LocalizedCacheTopology cacheTopology =
            manager(0).getCache(cacheName).getAdvancedCache().getDistributionManager().getCacheTopology();
      for (int i = 0; i < 1000; i++) {
         List<Address> addresses = cacheTopology.getDistribution(i).writeOwners();
         Object collocatedKey = keyAffinityService.getCollocatedKey(i);
         List<Address> addressList = cacheTopology.getDistribution(collocatedKey).writeOwners();
         assertEquals(addresses, addressList);
      }
   }

   public class KeyConsumer implements ExceptionRunnable {

      volatile Exception exception;


      private final int keysToConsume;
      private CountDownLatch consumersStart;
      private final List<Address> topology = topology();
      private final Random rnd = new Random();

      public KeyConsumer(int keysToConsume, CountDownLatch consumersStart) {
         this.keysToConsume = keysToConsume;
         this.consumersStart = consumersStart;
      }

      @Override
      public void run() {
         try {
            consumersStart.await();
         } catch (InterruptedException e) {
            log.debug("KeyConsumer thread interrupted");
            return;
         }
         for (int i = 0; i < keysToConsume; i++) {
            Address whichAddr = topology.get(rnd.nextInt(topology.size()));
            Object keyForAddress = keyAffinityService.getKeyForAddress(whichAddr);
            assertMapsToAddress(keyForAddress, whichAddr);
         }
      }
   }

}
