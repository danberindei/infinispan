package org.infinispan.api;

import org.infinispan.AdvancedCache;
import org.infinispan.commands.CreateCacheCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;

@Test(groups = "functional", testName = "api.ParallelCacheStartTest")
public class CreateCacheCommandTest extends MultipleCacheManagersTest {
   private final static int NUM_NODES = 4;
   private static final int NUM_ITERATIONS = 5;

   public CreateCacheCommandTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
      configurationBuilder.clustering().cacheMode(CacheMode.REPL_SYNC);
      for (int i = 0; i < NUM_NODES; i++) {
         addClusterEnabledCacheManager(configurationBuilder);
      }

      ConfigurationBuilder replNonTxConfig = createConfig(CacheMode.REPL_SYNC, TransactionMode.NON_TRANSACTIONAL);
      ConfigurationBuilder replTxConfig = createConfig(CacheMode.REPL_SYNC, TransactionMode.TRANSACTIONAL);
      ConfigurationBuilder distNonTxConfig = createConfig(CacheMode.DIST_SYNC, TransactionMode.NON_TRANSACTIONAL);
      ConfigurationBuilder distTxConfig = createConfig(CacheMode.DIST_SYNC, TransactionMode.TRANSACTIONAL);

      for (int i = 0; i < NUM_NODES; i++) {
         manager(i).defineConfiguration("replNonTx", replNonTxConfig.build());
         manager(i).defineConfiguration("replTx", replTxConfig.build());
         manager(i).defineConfiguration("distNonTx", distNonTxConfig.build());
         manager(i).defineConfiguration("distTx", distTxConfig.build());
      }
      waitForClusterToForm();
   }

   private ConfigurationBuilder createConfig(CacheMode cacheMode, TransactionMode transactionMode) {
      ConfigurationBuilder cfg = new ConfigurationBuilder();
      cfg.clustering().cacheMode(cacheMode);
      cfg.transaction().transactionMode(transactionMode);
      cfg.unsafe().unreliableReturnValues(true);
      return cfg;
   }

   public void testCreateReplNonTxCache() throws Exception {
      doTest("replNonTxCache");
   }

   public void testCreateReplTxCache() throws Exception {
      doTest("replTxCache");
   }

   public void testCreateDistNonTxCache() throws Exception {
      doTest("distNonTxCache");
   }

   public void testCreateDistTxCache() throws Exception {
      doTest("distTxCache");
   }

   private void doTest(String configName) throws Exception {
      for (int i = 0; i < NUM_ITERATIONS; i++) {
         String cacheName = configName + i;
         AdvancedCache<Object,Object> ownerCache = advancedCache(i % NUM_NODES);
         try {
            createCache(configName, cacheName, ownerCache);
            waitForClusterToForm(cacheName);
         } finally {
            ownerCache.getCacheManager().removeCache(cacheName);
         }
      }
   }

   private void createCache(String configName, String cacheName, final AdvancedCache<Object, Object> ownerCache)
         throws Exception {
      final CreateCacheCommand ccc = new CreateCacheCommand(ownerCache.getName(), cacheName, configName, true, NUM_NODES);
      Future<Object> future = fork(new Callable<Object>() {
         @Override
         public Object call() throws Exception {
            //locally
            ccc.init(ownerCache.getCacheManager());
            try {
               return ccc.perform(null);
            } catch (Throwable e) {
               throw new CacheException("Could not initialize temporary caches for MapReduce task on remote nodes ", e);
            }
         }
      });
      RpcManager rpc = ownerCache.getRpcManager();
      rpc.invokeRemotely(rpc.getMembers(), ccc, rpc.getDefaultRpcOptions(true));
      future.get();
   }
}
