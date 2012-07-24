package org.infinispan.tx.totalorder;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.transaction.TransactionProtocol;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

/**
 * A simple test with the cache operations for total order based protocol in distributed mode, when two phases are
 * needed
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "tx.totalorder.DistSimpleTotalOrder2PcTest")
public class DistSimpleTotalOrder2PcTest extends DistSimpleTotalOrder1PcTest {
   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder dcc = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      dcc.transaction().transactionProtocol(TransactionProtocol.TOTAL_ORDER);
      dcc.locking().isolationLevel(IsolationLevel.REPEATABLE_READ).writeSkewCheck(true)
            .versioning().enable().scheme(VersioningScheme.SIMPLE);
      dcc.clustering().hash().numOwners(2);
      createCluster(dcc, 2);
      waitForClusterToForm();
   }

   public void testRequiresVersioning() {
      assert cache(0).getConfiguration().isRequireVersioning();
   }
}
