package org.infinispan.lock.impl;

import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.jmx.CacheManagerJmxRegistration;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.lock.api.ClusteredLockManager;
import org.infinispan.lock.impl.entries.ClusteredLockKey;
import org.infinispan.lock.impl.entries.ClusteredLockValue;
import org.infinispan.lock.impl.functions.IsLocked;
import org.infinispan.lock.impl.functions.LockFunction;
import org.infinispan.lock.impl.functions.UnlockFunction;
import org.infinispan.lock.impl.lock.ClusteredLockFilter;
import org.infinispan.lock.impl.manager.CacheHolder;
import org.infinispan.lock.impl.manager.EmbeddedClusteredLockManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.partitionhandling.PartitionHandling;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.transaction.TransactionMode;
import org.kohsuke.MetaInfServices;

/**
 * Locks module configuration
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
@MetaInfServices(value = ModuleLifecycle.class)
public class ClusteredLockModuleLifecycle implements ModuleLifecycle {

   public static final String CLUSTERED_LOCK_CACHE_NAME = "org.infinispan.LOCKS";

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalConfiguration) {
      final Map<Integer, AdvancedExternalizer<?>> externalizerMap = globalConfiguration.serialization()
            .advancedExternalizers();
      externalizerMap.put(ClusteredLockKey.EXTERNALIZER.getId(), ClusteredLockKey.EXTERNALIZER);
      externalizerMap.put(ClusteredLockValue.EXTERNALIZER.getId(), ClusteredLockValue.EXTERNALIZER);
      externalizerMap.put(LockFunction.EXTERNALIZER.getId(), LockFunction.EXTERNALIZER);
      externalizerMap.put(UnlockFunction.EXTERNALIZER.getId(), UnlockFunction.EXTERNALIZER);
      externalizerMap.put(IsLocked.EXTERNALIZER.getId(), IsLocked.EXTERNALIZER);
      externalizerMap.put(ClusteredLockFilter.EXTERNALIZER.getId(), ClusteredLockFilter.EXTERNALIZER);
   }

   @Override
   public void cacheManagerStarted(GlobalComponentRegistry gcr) {
      final EmbeddedCacheManager cacheManager = gcr.getComponent(EmbeddedCacheManager.class);
      final InternalCacheRegistry internalCacheRegistry = gcr.getComponent(InternalCacheRegistry.class);

      internalCacheRegistry.registerInternalCache(CLUSTERED_LOCK_CACHE_NAME, createClusteredLockCacheConfiguration(),
            EnumSet.of(InternalCacheRegistry.Flag.EXCLUSIVE));

      ExecutorService executor = gcr.getComponent(KnownComponentNames.ASYNC_OPERATIONS_EXECUTOR);
      Future<CacheHolder> future = startCaches(cacheManager, executor);
      registerClusteredLockManager(gcr, future);
   }

   private static Configuration createClusteredLockCacheConfiguration() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.REPL_SYNC)
            .stateTransfer().fetchInMemoryState(true)
            .partitionHandling().whenSplit(PartitionHandling.DENY_READ_WRITES)
            .transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL);

      return builder.build();
   }

   private static Future<CacheHolder> startCaches(EmbeddedCacheManager cacheManager,
                                                  ExecutorService executor) {
      return executor.submit(() -> {
         Cache<? extends ClusteredLockKey, ClusteredLockValue> locksCache = cacheManager.getCache(CLUSTERED_LOCK_CACHE_NAME);

         return new CacheHolder(locksCache.getAdvancedCache());
      });
   }

   private static void registerClusteredLockManager(GlobalComponentRegistry registry, Future<CacheHolder> future) {
      //noinspection SynchronizationOnLocalVariableOrMethodParameter
      synchronized (registry) {
         ClusteredLockManager clusteredLockManager = registry.getComponent(ClusteredLockManager.class);
         if (clusteredLockManager == null || !(clusteredLockManager instanceof EmbeddedClusteredLockManager)) {
            clusteredLockManager = new EmbeddedClusteredLockManager(future);
            registry.registerComponent(clusteredLockManager, ClusteredLockManager.class);
            //this start() is only invoked when the DefaultCacheManager.start() is invoked
            //it is invoked here again to force it to check the managed global components
            // and register them in the MBeanServer, if they are missing.
            registry.getComponent(CacheManagerJmxRegistration.class).start(); //HACK!
         }
      }
   }

}
