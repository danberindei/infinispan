package org.infinispan.tx.totalorder;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.transaction.TransactionProtocol;
import org.infinispan.tx.totalorder.SimpleTotalOrder1PcTest;
import org.testng.annotations.Test;

/**
 * A simple test with the cache operations for total order based protocol in distributed mode
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "tx.totalorder.DistSimpleTotalOrder1PcTest")
public class DistSimpleTotalOrder1PcTest extends SimpleTotalOrder1PcTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder dcc = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      dcc.transaction().transactionProtocol(TransactionProtocol.TOTAL_ORDER);
      dcc.clustering().hash().numOwners(2);
      createCluster(dcc, clusterSize);
      waitForClusterToForm();
   }
}
