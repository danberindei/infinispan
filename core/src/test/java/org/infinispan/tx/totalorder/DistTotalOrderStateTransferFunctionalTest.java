package org.infinispan.tx.totalorder;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.statetransfer.StateTransferFunctionalTest;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.transaction.TransactionProtocol;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * @author Mircea Markus <mircea.markus@jboss.com> (C) 2011 Red Hat Inc.
 * @since 5.2
 */
@Test(groups = "functional", testName = "tx.totalorder.DistTotalOrderStateTransferFunctionalTest")
public class DistTotalOrderStateTransferFunctionalTest extends StateTransferFunctionalTest {

   private ConfigurationBuilder dcc;

   protected void createCacheManagers() throws Throwable {
      dcc = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      dcc.transaction().transactionProtocol(TransactionProtocol.TOTAL_ORDER);
      dcc.locking().isolationLevel(IsolationLevel.REPEATABLE_READ).writeSkewCheck(true)
            .versioning().enable().scheme(VersioningScheme.SIMPLE);
      dcc.clustering().stateTransfer().fetchInMemoryState(true);
   }

   protected EmbeddedCacheManager createCacheManager() {
      EmbeddedCacheManager cm = addClusterEnabledCacheManager(new TransportFlags().withMerge(true));
      cm.defineConfiguration(cacheName, dcc.build());
      return cm;
   }

   public void testClusterFormation() {
      EmbeddedCacheManager cm0 = createCacheManager();
      EmbeddedCacheManager cm1 = createCacheManager();
      final Cache<Object, Object> cache0 = cm0.getCache(cacheName);
      final Cache<Object, Object> cache1 = cm1.getCache(cacheName);
      cache0.put("k", "v");
      assertEquals(cache0.get("k"), "v");
      assertEventuallyEquals(cache1, "k", "v");
   }
}
