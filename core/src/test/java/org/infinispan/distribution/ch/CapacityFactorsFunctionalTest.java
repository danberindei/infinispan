package org.infinispan.distribution.ch;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.ch.impl.DefaultConsistentHash;
import org.infinispan.distribution.ch.impl.OwnershipStatistics;
import org.infinispan.distribution.group.PartitionerConsistentHash;
import org.infinispan.distribution.ch.impl.SyncConsistentHashFactory;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import java.util.Map;

import static org.testng.AssertJUnit.assertEquals;

/**
 * Test the capacity factors with the full stack.
 *
 * @author Dan Berindei
 * @since 6.0
 */
@Test(groups = "functional", testName = "distribution.ch.CapacityFactorsFunctionalTest")
public class CapacityFactorsFunctionalTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      // Do nothing here, create the cache managers in the test
   }

   public void testCapacityFactors() {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.DIST_SYNC);
      cb.clustering().hash().numSegments(60);

      cb.clustering().hash().capacityFactor(0.5f);
      addClusterEnabledCacheManager(cb);
      waitForClusterToForm();
      assertCapacityFactors(0.5f);
      assertPrimaryOwned(60);
      assertOwned(60);

      cb.clustering().hash().capacityFactor(1.5f);
      addClusterEnabledCacheManager(cb);
      waitForClusterToForm();
      assertCapacityFactors(0.5f, 1.5f);
      assertPrimaryOwned(15, 45);
      assertOwned(60, 60);

      cb.clustering().hash().capacityFactor(0.0f);
      addClusterEnabledCacheManager(cb);
      waitForClusterToForm();
      assertCapacityFactors(0.5f, 1.5f, 0.0f);
      assertPrimaryOwned(15, 45);
      assertOwned(60, 60, 0);

      cb.clustering().hash().capacityFactor(1.0f);
      addClusterEnabledCacheManager(cb);
      waitForClusterToForm();
      assertCapacityFactors(0.5f, 1.5f, 0.0f, 1.0f);
      assertPrimaryOwned(10, 30, 0, 20);
      assertOwned(20, 60, 0, 40);
   }

   private void assertCapacityFactors(float... expectedCapacityFactors) {
      ConsistentHash ch = cache(0).getAdvancedCache().getDistributionManager().getReadConsistentHash();
      DefaultConsistentHash dch =
            (DefaultConsistentHash) TestingUtil.extractField(PartitionerConsistentHash.class, ch, "ch");
      int numNodes = expectedCapacityFactors.length;
      Map<Address,Float> capacityFactors = dch.getCapacityFactors();
      for (int i = 0; i < numNodes; i++) {
         assertEquals(expectedCapacityFactors[i], capacityFactors.get(address(i)), 0.0);
      }
   }

   private void assertPrimaryOwned(int... expectedPrimaryOwned) {
      ConsistentHash ch = cache(0).getAdvancedCache().getDistributionManager().getReadConsistentHash();
      OwnershipStatistics stats = new OwnershipStatistics(ch, ch.getMembers());
      int numNodes = expectedPrimaryOwned.length;
      for (int i = 0; i < numNodes; i++) {
         double maxDeviation = SyncConsistentHashFactory.OWNED_SEGMENTS_ALLOWED_VARIATION * expectedPrimaryOwned[i] + 1;
         assertEquals((double) expectedPrimaryOwned[i], (double) stats.getPrimaryOwned(address(i)), maxDeviation);
      }
   }

   private void assertOwned(int... expectedOwned) {
      ConsistentHash ch = cache(0).getAdvancedCache().getDistributionManager().getReadConsistentHash();
      OwnershipStatistics stats = new OwnershipStatistics(ch, ch.getMembers());
      int numNodes = expectedOwned.length;
      for (int i = 0; i < numNodes; i++) {
         double maxDeviation = SyncConsistentHashFactory.OWNED_SEGMENTS_ALLOWED_VARIATION * expectedOwned[i] + 1;
         assertEquals((double)expectedOwned[i], (double)stats.getOwned(address(i)), maxDeviation);
      }
   }
}
