 package org.infinispan.configuration.parsing;

import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertSame;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.infinispan.Version;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.commons.executors.BlockingThreadPoolExecutorFactory;
import org.infinispan.commons.executors.CachedThreadPoolExecutorFactory;
import org.infinispan.commons.executors.ScheduledThreadPoolExecutorFactory;
import org.infinispan.commons.executors.ThreadPoolExecutorFactory;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller;
import org.infinispan.configuration.QueryableDataContainer;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.BackupFailurePolicy;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ClusterLoaderConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.EncodingConfiguration;
import org.infinispan.configuration.cache.Index;
import org.infinispan.configuration.cache.InterceptorConfiguration;
import org.infinispan.configuration.cache.MemoryConfiguration;
import org.infinispan.configuration.cache.PartitionHandlingConfiguration;
import org.infinispan.configuration.cache.PersistenceConfiguration;
import org.infinispan.configuration.cache.SingleFileStoreConfiguration;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalStateConfiguration;
import org.infinispan.configuration.global.ShutdownHookBehavior;
import org.infinispan.conflict.MergePolicy;
import org.infinispan.distribution.ch.impl.SyncConsistentHashFactory;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionType;
import org.infinispan.factories.threads.DefaultThreadFactory;
import org.infinispan.globalstate.ConfigurationStorage;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.interceptors.impl.InvocationContextInterceptor;
import org.infinispan.jmx.CustomMBeanServerPropertiesTest;
import org.infinispan.marshall.AdvancedExternalizerTest;
import org.infinispan.marshall.TestObjectStreamMarshaller;
import org.infinispan.partitionhandling.PartitionHandling;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfiguration;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.JBossStandaloneJTAManagerLookup;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "configuration.parsing.UnifiedXmlFileParsingTest")
public class UnifiedXmlFileParsingTest extends AbstractInfinispanTest {

   @DataProvider(name = "configurationFiles")
   public Object[][] configurationFiles() {
      return new Object[][] { {"7.0.xml"}, {"7.1.xml"}, {"7.2.xml"}, {"8.0.xml"}, {"8.1.xml"}, {"8.2.xml"}, {"9.0.xml"}, {"9.1.xml"}, {"9.2.xml"}, {"9.3.xml"} };
   }

   @Test(dataProvider="configurationFiles")
   public void testParseAndConstructUnifiedXmlFile(String config) throws IOException {
      String[] parts = config.split("\\.");
      int major = Integer.parseInt(parts[0]);
      int minor = Integer.parseInt(parts[1]);
      final int version = major * 10 + minor;
      ParserRegistry parserRegistry = new ParserRegistry(Thread.currentThread().getContextClassLoader(), true);
      ConfigurationBuilderHolder holder = parserRegistry.parseFile("configs/unified/" + config);
      switch (version) {
         case 93:
            configurationCheck93(holder);
         case 92:
            configurationCheck92(holder);
            break;
         case 91:
            configurationCheck91(holder);
            break;
         case 90:
            configurationCheck90(holder);
            break;
         case 82:
            configurationCheck82(holder);
            break;
         case 81:
            configurationCheck81(holder);
            break;
         case 80:
            configurationCheck80(holder);
            break;
         case 70:
         case 71:
         case 72:
            configurationCheck70(holder);
            break;
         default:
            throw new IllegalArgumentException("Unknown configuration version "+version);
      }
   }

   private static void configurationCheck93(ConfigurationBuilderHolder holder) {
      configurationCheck92(holder);
      Configuration local = holder.getNamedConfigurationBuilders().get("local").build();
      PersistenceConfiguration persistenceConfiguration = local.persistence();
      assertEquals(5, persistenceConfiguration.connectionAttempts());
      assertEquals(100, persistenceConfiguration.connectionInterval());
      assertEquals(2000, persistenceConfiguration.availabilityInterval());
      assertFalse(persistenceConfiguration.stores().isEmpty());
      AsyncStoreConfiguration asyncConfig = persistenceConfiguration.stores().iterator().next().async();
      assertTrue(asyncConfig.failSilently());
   }

   private static void configurationCheck92(ConfigurationBuilderHolder holder) {
      configurationCheck91(holder);
      GlobalConfiguration g = holder.getGlobalConfigurationBuilder().build();
      GlobalStateConfiguration gs = g.globalState();
      assertEquals(ConfigurationStorage.OVERLAY, gs.configurationStorage());
      assertEquals("sharedPath", gs.sharedPersistentLocation());

      EncodingConfiguration encoding = holder.getNamedConfigurationBuilders().get("local").build().encoding();
      assertEquals(MediaType.APPLICATION_OBJECT, encoding.keyDataType().mediaType());
      assertEquals(MediaType.APPLICATION_OBJECT, encoding.valueDataType().mediaType());

      MemoryConfiguration memory = holder.getNamedConfigurationBuilders().get("dist-template").build().memory();
      assertEquals(EvictionStrategy.REMOVE, memory.evictionStrategy());
   }

   private static void configurationCheck91(ConfigurationBuilderHolder holder) {
      configurationCheck90(holder);
      ConfigurationBuilder dist = holder.getNamedConfigurationBuilders().get("dist");
      PartitionHandlingConfiguration ph = dist.build().clustering().partitionHandling();
      assertTrue(ph.enabled());
      assertEquals(PartitionHandling.ALLOW_READS, ph.whenSplit());
      assertEquals(MergePolicy.PREFERRED_NON_NULL, ph.mergePolicy());

      ConfigurationBuilder repl = holder.getNamedConfigurationBuilders().get("repl");
      ph = repl.build().clustering().partitionHandling();
      assertFalse(ph.enabled());
      assertEquals(PartitionHandling.ALLOW_READ_WRITES, ph.whenSplit());
      assertEquals(MergePolicy.NONE, ph.mergePolicy());
   }

   private static void configurationCheck90(ConfigurationBuilderHolder holder) {
      configurationCheck82(holder);
      GlobalConfiguration globalConfiguration = holder.getGlobalConfigurationBuilder().build();
      assertEquals(4, globalConfiguration.transport().initialClusterSize());
      assertEquals(30000, globalConfiguration.transport().initialClusterTimeout());

      MemoryConfiguration mc = holder.getNamedConfigurationBuilders().get("off-heap-memory").build().memory();
      assertEquals(StorageType.OFF_HEAP, mc.storageType());
      assertEquals(10000000, mc.size());
      assertEquals(4, mc.addressCount());
      assertEquals(EvictionType.MEMORY, mc.evictionType());

      mc = holder.getNamedConfigurationBuilders().get("binary-memory").build().memory();
      assertEquals(StorageType.BINARY, mc.storageType());
      assertEquals(1, mc.size());

      mc = holder.getNamedConfigurationBuilders().get("object-memory").build().memory();
      assertEquals(StorageType.OBJECT, mc.storageType());
   }

   private static void configurationCheck82(ConfigurationBuilderHolder holder) {
      configurationCheck81(holder);
      GlobalConfiguration globalConfiguration = holder.getGlobalConfigurationBuilder().build();
      assertEquals(4, globalConfiguration.transport().initialClusterSize());
      assertEquals(30000, globalConfiguration.transport().initialClusterTimeout());
   }

   private static void configurationCheck81(ConfigurationBuilderHolder holder) {
      configurationCheck80(holder);
      GlobalConfiguration globalConfiguration = holder.getGlobalConfigurationBuilder().build();
      assertTrue(globalConfiguration.globalState().enabled());
      assertEquals("persistentPath", globalConfiguration.globalState().persistentLocation());
      assertEquals("tmpPath", globalConfiguration.globalState().temporaryLocation());
   }

   private static void configurationCheck80(ConfigurationBuilderHolder holder) {
      configurationCheck70(holder);
      Configuration c = holder.getDefaultConfigurationBuilder().build();
      assertFalse(c.memory().evictionType() == EvictionType.MEMORY);
      c = holder.getNamedConfigurationBuilders().get("invalid").build();
      assertTrue(c.memory().evictionType() == EvictionType.COUNT);

      DefaultThreadFactory threadFactory;
      BlockingThreadPoolExecutorFactory threadPool;

      GlobalConfiguration g = holder.getGlobalConfigurationBuilder().build();
      threadFactory = g.asyncThreadPool().threadFactory();
      assertEquals("infinispan", threadFactory.threadGroup().getName());
      assertEquals("thread-%c-%n-p%f-t%t", threadFactory.threadNamePattern());
      assertEquals(5, threadFactory.initialPriority());
      threadPool = g.asyncThreadPool().threadPoolFactory();
      assertEquals(5, threadPool.coreThreads());
      assertEquals(5, threadPool.maxThreads());
      assertEquals(0, threadPool.queueLength());
      assertEquals(0, threadPool.keepAlive());

      threadFactory = g.stateTransferThreadPool().threadFactory();
      assertEquals("infinispan", threadFactory.threadGroup().getName());
      assertEquals("thread-%c-%n-p%f-t%t", threadFactory.threadNamePattern());
      assertEquals(5, threadFactory.initialPriority());
      threadPool = g.stateTransferThreadPool().threadPoolFactory();
      assertEquals(1, threadPool.coreThreads());
      assertEquals(60, threadPool.maxThreads());
      assertEquals(0, threadPool.queueLength());
      assertEquals(0, threadPool.keepAlive());

      assertTemplateConfiguration(holder, "local-template");
      assertTemplateConfiguration(holder, "invalidation-template");
      assertTemplateConfiguration(holder, "repl-template");
      assertTemplateConfiguration(holder, "dist-template");

      assertCacheConfiguration(holder, "local-instance");
      assertCacheConfiguration(holder, "invalidation-instance");
      assertCacheConfiguration(holder, "repl-instance");
      assertCacheConfiguration(holder, "dist-instance");

      Configuration localTemplate = holder.getNamedConfigurationBuilders().get("local-template").build();
      Configuration localConfiguration = holder.getNamedConfigurationBuilders().get("local-instance").build();
      assertEquals(10000, localTemplate.expiration().wakeUpInterval());
      assertEquals(11000, localConfiguration.expiration().wakeUpInterval());
      assertEquals(10, localTemplate.expiration().lifespan());
      assertEquals(10, localConfiguration.expiration().lifespan());

      Configuration replTemplate = holder.getNamedConfigurationBuilders().get("repl-template").build();
      Configuration replConfiguration = holder.getNamedConfigurationBuilders().get("repl-instance").build();
      assertEquals(31000, replTemplate.locking().lockAcquisitionTimeout());
      assertEquals(32000, replConfiguration.locking().lockAcquisitionTimeout());
      assertEquals(3000, replTemplate.locking().concurrencyLevel());
      assertEquals(3000, replConfiguration.locking().concurrencyLevel());
   }

   private static void configurationCheck70(ConfigurationBuilderHolder holder) {
      GlobalConfiguration g = holder.getGlobalConfigurationBuilder().build();
      assertEquals("maximal", g.globalJmxStatistics().cacheManagerName());
      assertTrue(g.globalJmxStatistics().enabled());
      assertEquals("my-domain", g.globalJmxStatistics().domain());
      assertTrue(g.globalJmxStatistics().mbeanServerLookup() instanceof CustomMBeanServerPropertiesTest.TestLookup);
      assertEquals(1, g.globalJmxStatistics().properties().size());
      assertEquals("value", g.globalJmxStatistics().properties().getProperty("key"));

      // Transport
      assertEquals("maximal-cluster", g.transport().clusterName());
      assertEquals(120000, g.transport().distributedSyncTimeout());
      assertEquals("udp", g.transport().properties().getProperty("stack-udp"));
      assertEquals("tcp", g.transport().properties().getProperty("stack-tcp"));
      assertEquals("jgroups-udp.xml", g.transport().properties().getProperty("stackFilePath-udp"));
      assertEquals("jgroups-tcp.xml", g.transport().properties().getProperty("stackFilePath-tcp"));
      assertEquals("tcp", g.transport().properties().getProperty("stack"));

      DefaultThreadFactory threadFactory;
      BlockingThreadPoolExecutorFactory blockingThreadPool;

      threadFactory = g.listenerThreadPool().threadFactory();
      assertEquals("infinispan", threadFactory.threadGroup().getName());
      assertEquals("thread-%c-%n-p%f-t%t", threadFactory.threadNamePattern());
      assertEquals(5, threadFactory.initialPriority());
      blockingThreadPool = g.listenerThreadPool().threadPoolFactory();
      assertEquals(1, blockingThreadPool.coreThreads());
      assertEquals(1, blockingThreadPool.maxThreads());
      assertEquals(0, blockingThreadPool.queueLength());
      assertEquals(0, blockingThreadPool.keepAlive());

      assertTrue(g.expirationThreadPool().threadPoolFactory() instanceof ScheduledThreadPoolExecutorFactory);
      threadFactory = g.expirationThreadPool().threadFactory();
      assertEquals("infinispan", threadFactory.threadGroup().getName());
      assertEquals("thread-%c-%n-p%f-t%t", threadFactory.threadNamePattern());
      assertEquals(5, threadFactory.initialPriority());

      ThreadPoolExecutorFactory threadPoolExecutorFactory = g.replicationQueueThreadPool().threadPoolFactory();
      if (threadPoolExecutorFactory != null) { // Removed on 9.0
         assertTrue(threadPoolExecutorFactory instanceof ScheduledThreadPoolExecutorFactory);
         threadFactory = g.replicationQueueThreadPool().threadFactory();
         assertEquals("infinispan", threadFactory.threadGroup().getName());
         assertEquals("%G %i", threadFactory.threadNamePattern());
         assertEquals(5, threadFactory.initialPriority());
      }

      threadFactory = g.transport().remoteCommandThreadPool().threadFactory();
      assertEquals("infinispan", threadFactory.threadGroup().getName());
      assertEquals("thread-%c-%n-p%f-t%t", threadFactory.threadNamePattern());
      assertEquals(5, threadFactory.initialPriority());
      assertSame(CachedThreadPoolExecutorFactory.INSTANCE, g.transport().remoteCommandThreadPool().threadPoolFactory());

      threadFactory = g.transport().transportThreadPool().threadFactory();
      assertEquals("infinispan", threadFactory.threadGroup().getName());
      assertEquals("thread-%c-%n-p%f-t%t", threadFactory.threadNamePattern());
      assertEquals(5, threadFactory.initialPriority());
      blockingThreadPool = g.transport().transportThreadPool().threadPoolFactory();
      assertEquals(5, blockingThreadPool.coreThreads());
      assertEquals(10, blockingThreadPool.maxThreads());
      assertEquals(100, blockingThreadPool.queueLength());
      assertEquals(10000, blockingThreadPool.keepAlive());

      assertTrue(g.serialization().marshaller() instanceof TestObjectStreamMarshaller);
      assertEquals(Version.getVersionShort("1.0"), g.serialization().version());
      Map<Integer, AdvancedExternalizer<?>> externalizers = g.serialization().advancedExternalizers();
      AdvancedExternalizer<?> externalizer = externalizers.get(9001);
      assertTrue(externalizer instanceof AdvancedExternalizerTest.IdViaConfigObj.Externalizer);
      externalizer = externalizers.get(9002);
      assertTrue(externalizer instanceof AdvancedExternalizerTest.IdViaAnnotationObj.Externalizer);
      assertEquals(ShutdownHookBehavior.DONT_REGISTER, g.shutdown().hookBehavior());

      // Default cache is "local" named cache
      Configuration c = holder.getDefaultConfigurationBuilder().build();
      assertFalse(c.invocationBatching().enabled());
      assertTrue(c.jmxStatistics().enabled());
      assertEquals(CacheMode.LOCAL, c.clustering().cacheMode());
      assertEquals(30000, c.locking().lockAcquisitionTimeout());
      assertEquals(2000, c.locking().concurrencyLevel());
      assertEquals(IsolationLevel.NONE, c.locking().isolationLevel());
      assertTrue(c.locking().useLockStriping());
      assertEquals(TransactionMode.TRANSACTIONAL, c.transaction().transactionMode()); // Full XA
      assertFalse(c.transaction().useSynchronization()); // Full XA
      assertTrue(c.transaction().recovery().enabled()); // Full XA
      assertEquals(LockingMode.OPTIMISTIC, c.transaction().lockingMode());
      assertTrue(c.transaction().transactionManagerLookup() instanceof JBossStandaloneJTAManagerLookup);
      assertEquals(60000, c.transaction().cacheStopTimeout());
      assertEquals(20000, c.memory().size());
      assertEquals(10000, c.expiration().wakeUpInterval());
      assertEquals(10, c.expiration().lifespan());
      assertEquals(10, c.expiration().maxIdle());
      assertFalse(c.persistence().passivation());
      SingleFileStoreConfiguration fileStore = (SingleFileStoreConfiguration) c.persistence().stores().get(0);
      assertFalse(fileStore.fetchPersistentState());
      assertEquals("path", fileStore.location());
      assertFalse(fileStore.singletonStore().enabled());
      assertFalse(fileStore.purgeOnStartup());
      assertTrue(fileStore.preload());
      assertFalse(fileStore.shared());
      assertEquals(2048, fileStore.async().modificationQueueSize());
      assertEquals(1, fileStore.async().threadPoolSize());
      assertEquals(Index.NONE, c.indexing().index());

      c = holder.getNamedConfigurationBuilders().get("invalid").build();
      assertEquals(CacheMode.INVALIDATION_SYNC, c.clustering().cacheMode());
      assertTrue(c.invocationBatching().enabled());
      assertTrue(c.jmxStatistics().enabled());
      assertEquals(30500, c.locking().lockAcquisitionTimeout());
      assertEquals(2500, c.locking().concurrencyLevel());
      assertEquals(IsolationLevel.READ_COMMITTED, c.locking().isolationLevel()); // Converted to READ_COMMITTED by builder
      assertTrue(c.locking().useLockStriping());
      assertEquals(TransactionMode.TRANSACTIONAL, c.transaction().transactionMode()); // Non XA
      assertTrue(c.transaction().useSynchronization()); // Non XA
      assertFalse(c.transaction().recovery().enabled()); // Non XA
      assertEquals(LockingMode.OPTIMISTIC, c.transaction().lockingMode());
      assertEquals(60500, c.transaction().cacheStopTimeout());
      assertEquals(20500, c.memory().size());
      assertEquals(10500, c.expiration().wakeUpInterval());
      assertEquals(11, c.expiration().lifespan());
      assertEquals(11, c.expiration().maxIdle());
      assertEquals(Index.NONE, c.indexing().index());

      c = holder.getNamedConfigurationBuilders().get("repl").build();
      assertEquals(CacheMode.REPL_SYNC, c.clustering().cacheMode());
      assertTrue(c.invocationBatching().enabled());
      assertTrue(c.jmxStatistics().enabled());
      assertEquals(31000, c.locking().lockAcquisitionTimeout());
      assertEquals(3000, c.locking().concurrencyLevel());
      assertEquals(IsolationLevel.REPEATABLE_READ, c.locking().isolationLevel()); // Converted to REPEATABLE_READ by builder
      assertTrue(c.locking().useLockStriping());
      assertEquals(TransactionMode.TRANSACTIONAL, c.transaction().transactionMode()); // Batching, non XA
      assertTrue(c.transaction().useSynchronization()); // Batching, non XA
      assertFalse(c.transaction().recovery().enabled()); // Batching, non XA
      assertEquals(LockingMode.PESSIMISTIC, c.transaction().lockingMode());
      assertEquals(61000, c.transaction().cacheStopTimeout());
      assertEquals(21000, c.memory().size());
      assertEquals(11000, c.expiration().wakeUpInterval());
      assertEquals(12, c.expiration().lifespan());
      assertEquals(12, c.expiration().maxIdle());
      assertFalse(c.clustering().stateTransfer().fetchInMemoryState());
      assertEquals(60000, c.clustering().stateTransfer().timeout());
      assertEquals(10000, c.clustering().stateTransfer().chunkSize());
      ClusterLoaderConfiguration clusterLoader = getStoreConfiguration(c, ClusterLoaderConfiguration.class);
      assertEquals(35000, clusterLoader.remoteCallTimeout());
      assertFalse(clusterLoader.preload());
      assertEquals(Index.NONE, c.indexing().index());

      c = holder.getNamedConfigurationBuilders().get("dist").build();
      assertEquals(CacheMode.DIST_SYNC, c.clustering().cacheMode());
      assertFalse(c.invocationBatching().enabled());
      assertEquals(1200000, c.clustering().l1().lifespan());
      assertEquals(4, c.clustering().hash().numOwners());
      assertEquals(35000, c.clustering().remoteTimeout());
      assertEquals(2, c.clustering().hash().numSegments());
      assertTrue(c.clustering().hash().consistentHashFactory() instanceof SyncConsistentHashFactory);
      assertTrue(c.clustering().partitionHandling().enabled());
      assertTrue(c.jmxStatistics().enabled());
      assertEquals(31500, c.locking().lockAcquisitionTimeout());
      assertEquals(3500, c.locking().concurrencyLevel());
      assertEquals(IsolationLevel.READ_COMMITTED, c.locking().isolationLevel());
      assertTrue(c.locking().useLockStriping());
      assertEquals(TransactionMode.TRANSACTIONAL, c.transaction().transactionMode()); // Full XA
      assertFalse(c.transaction().useSynchronization()); // Full XA
      assertTrue(c.transaction().recovery().enabled()); // Full XA
      assertEquals(LockingMode.OPTIMISTIC, c.transaction().lockingMode());
      assertEquals(61500, c.transaction().cacheStopTimeout());
      assertEquals(21500, c.memory().size());
      assertEquals(11500, c.expiration().wakeUpInterval());
      assertEquals(13, c.expiration().lifespan());
      assertEquals(13, c.expiration().maxIdle());
      assertTrue(c.clustering().stateTransfer().fetchInMemoryState());
      assertEquals(60500, c.clustering().stateTransfer().timeout());
      assertEquals(10500, c.clustering().stateTransfer().chunkSize());
      // Back up cross-site configuration
      BackupConfiguration backup = c.sites().allBackups().get(0);
      assertEquals("NYC", backup.site());
      assertEquals(BackupFailurePolicy.WARN, backup.backupFailurePolicy());
      assertEquals(BackupConfiguration.BackupStrategy.SYNC, backup.strategy());
      assertEquals(12500, backup.replicationTimeout());
      assertFalse(backup.enabled());
      backup = c.sites().allBackups().get(1);
      assertEquals("SFO", backup.site());
      assertEquals(BackupFailurePolicy.IGNORE, backup.backupFailurePolicy());
      assertEquals(BackupConfiguration.BackupStrategy.ASYNC, backup.strategy());
      assertEquals(13000, backup.replicationTimeout());
      assertTrue(backup.enabled());
      backup = c.sites().allBackups().get(2);
      assertEquals("LON", backup.site());
      assertEquals(BackupFailurePolicy.FAIL, backup.backupFailurePolicy());
      assertEquals(BackupConfiguration.BackupStrategy.SYNC, backup.strategy());
      assertEquals(13500, backup.replicationTimeout());
      assertTrue(backup.enabled());
      assertEquals(3, backup.takeOffline().afterFailures());
      assertEquals(10000, backup.takeOffline().minTimeToWait());
      assertEquals("users", c.sites().backupFor().remoteCache());
      assertEquals("LON", c.sites().backupFor().remoteSite());

      c = holder.getNamedConfigurationBuilders().get("capedwarf-data").build();
      assertEquals(CacheMode.REPL_SYNC, c.clustering().cacheMode());
      assertEquals(TransactionMode.TRANSACTIONAL, c.transaction().transactionMode()); // Non XA
      assertTrue(c.transaction().useSynchronization()); // Non XA
      assertFalse(c.transaction().recovery().enabled()); // Non XA
      assertEquals(StorageType.OBJECT, c.memory().storageType());
      assertEquals(-1, c.memory().size());
      fileStore = getStoreConfiguration(c, SingleFileStoreConfiguration.class);
      assertTrue(fileStore.preload());
      assertFalse(fileStore.purgeOnStartup());

      c = holder.getNamedConfigurationBuilders().get("capedwarf-metadata").build();
      assertEquals(CacheMode.REPL_SYNC, c.clustering().cacheMode());
      assertEquals(TransactionMode.TRANSACTIONAL, c.transaction().transactionMode()); // Non XA
      assertTrue(c.transaction().useSynchronization()); // Non XA
      assertFalse(c.transaction().recovery().enabled()); // Non XA
      assertEquals(StorageType.OBJECT, c.memory().storageType());
      assertEquals(-1, c.memory().size());
      DummyInMemoryStoreConfiguration dummyStore = getStoreConfiguration(c, DummyInMemoryStoreConfiguration.class);
      assertFalse(dummyStore.preload());
      assertFalse(dummyStore.purgeOnStartup());

      c = holder.getNamedConfigurationBuilders().get("capedwarf-memcache").build();
      assertEquals(CacheMode.REPL_SYNC, c.clustering().cacheMode());
      assertEquals(TransactionMode.TRANSACTIONAL, c.transaction().transactionMode()); // Non XA
      assertTrue(c.transaction().useSynchronization()); // Non XA
      assertFalse(c.transaction().recovery().enabled()); // Non XA
      assertEquals(StorageType.OBJECT, c.memory().storageType());
      assertEquals(-1, c.memory().size());
      assertEquals(LockingMode.PESSIMISTIC, c.transaction().lockingMode());

      c = holder.getNamedConfigurationBuilders().get("capedwarf-default").build();
      assertEquals(CacheMode.DIST_SYNC, c.clustering().cacheMode());
      assertEquals(TransactionMode.TRANSACTIONAL, c.transaction().transactionMode()); // Non XA
      assertTrue(c.transaction().useSynchronization()); // Non XA
      assertFalse(c.transaction().recovery().enabled()); // Non XA
      assertEquals(StorageType.OBJECT, c.memory().storageType());
      assertEquals(-1, c.memory().size());
      fileStore = getStoreConfiguration(c, SingleFileStoreConfiguration.class);
      assertTrue(fileStore.preload());
      assertFalse(fileStore.purgeOnStartup());
      assertEquals(Index.NONE, c.indexing().index());

      c = holder.getNamedConfigurationBuilders().get("capedwarf-dist").build();
      assertEquals(CacheMode.DIST_SYNC, c.clustering().cacheMode());
      assertEquals(TransactionMode.TRANSACTIONAL, c.transaction().transactionMode()); // Non XA
      assertTrue(c.transaction().useSynchronization()); // Non XA
      assertFalse(c.transaction().recovery().enabled()); // Non XA
      assertEquals(StorageType.OBJECT, c.memory().storageType());
      assertEquals(-1, c.memory().size());
      assertEquals(LockingMode.PESSIMISTIC, c.transaction().lockingMode());
      fileStore = getStoreConfiguration(c, SingleFileStoreConfiguration.class);
      assertTrue(fileStore.preload());
      assertFalse(fileStore.purgeOnStartup());

      c = holder.getNamedConfigurationBuilders().get("capedwarf-tasks").build();
      assertEquals(CacheMode.DIST_SYNC, c.clustering().cacheMode());
      assertEquals(TransactionMode.TRANSACTIONAL, c.transaction().transactionMode()); // Non XA
      assertTrue(c.transaction().useSynchronization()); // Non XA
      assertFalse(c.transaction().recovery().enabled()); // Non XA
      assertEquals(10000, c.memory().size());
      fileStore = getStoreConfiguration(c, SingleFileStoreConfiguration.class);
      assertTrue(fileStore.preload());
      assertFalse(fileStore.purgeOnStartup());
      assertEquals(Index.NONE, c.indexing().index());

      c = holder.getNamedConfigurationBuilders().get("HibernateSearch-LuceneIndexesMetadata").build();
      assertEquals(CacheMode.REPL_SYNC, c.clustering().cacheMode());
      assertEquals(TransactionMode.TRANSACTIONAL, c.transaction().transactionMode()); // Non XA
      assertTrue(c.invocationBatching().enabled());
      assertTrue(c.transaction().useSynchronization()); // Non XA
      assertFalse(c.transaction().recovery().enabled()); // Non XA
      assertEquals(-1, c.memory().size());
      fileStore = getStoreConfiguration(c, SingleFileStoreConfiguration.class);
      assertTrue(fileStore.preload());
      assertFalse(fileStore.purgeOnStartup());

      c = holder.getNamedConfigurationBuilders().get("HibernateSearch-LuceneIndexesData").build();
      assertEquals(CacheMode.REPL_SYNC, c.clustering().cacheMode());
      assertEquals(TransactionMode.TRANSACTIONAL, c.transaction().transactionMode()); // Non XA
      assertTrue(c.invocationBatching().enabled());
      assertTrue(c.transaction().useSynchronization()); // Non XA
      assertFalse(c.transaction().recovery().enabled()); // Non XA
      assertEquals(-1, c.memory().size());
      fileStore = getStoreConfiguration(c, SingleFileStoreConfiguration.class);
      assertTrue(fileStore.preload());
      assertFalse(fileStore.purgeOnStartup());

      c = holder.getNamedConfigurationBuilders().get("HibernateSearch-LuceneIndexesLocking").build();
      assertEquals(CacheMode.REPL_SYNC, c.clustering().cacheMode());
      assertEquals(TransactionMode.TRANSACTIONAL, c.transaction().transactionMode()); // Non XA
      assertTrue(c.invocationBatching().enabled());
      assertTrue(c.transaction().useSynchronization()); // Non XA
      assertFalse(c.transaction().recovery().enabled()); // Non XA
      assertEquals(-1, c.memory().size());

      c = holder.getNamedConfigurationBuilders().get("custom-interceptors").build();
      List<InterceptorConfiguration> interceptors = c.customInterceptors().interceptors();
      InterceptorConfiguration interceptor = interceptors.get(0);
      assertTrue(interceptor.interceptor() instanceof CustomInterceptor1);
      assertEquals(InvocationContextInterceptor.class, interceptor.after());
      interceptor = interceptors.get(1);
      assertEquals(InvocationContextInterceptor.class, interceptor.before());
      assertTrue(interceptor.interceptor() instanceof CustomInterceptor2);
      interceptor = interceptors.get(2);
      assertTrue(interceptor.interceptor() instanceof CustomInterceptor3);
      assertEquals(1, interceptor.index());
      interceptor = interceptors.get(3);
      assertTrue(interceptor.interceptor() instanceof CustomInterceptor4);
      assertEquals(InterceptorConfiguration.Position.LAST, interceptor.position());
      assertTrue(c.unsafe().unreliableReturnValues());

      c = holder.getNamedConfigurationBuilders().get("write-skew").build();
      assertEquals(IsolationLevel.REPEATABLE_READ, c.locking().isolationLevel());
      assertTrue(c.versioning().enabled());
      assertEquals(VersioningScheme.SIMPLE, c.versioning().scheme());
      assertFalse(c.deadlockDetection().enabled());

      c = holder.getNamedConfigurationBuilders().get("compatibility").build();
      assertTrue(c.compatibility().enabled());
      assertTrue(c.compatibility().marshaller() instanceof GenericJBossMarshaller);
      assertFalse(c.deadlockDetection().enabled());
      assertEquals(-1, c.deadlockDetection().spinDuration());

      c = holder.getNamedConfigurationBuilders().get("custom-container").build();
      assertTrue(c.dataContainer().dataContainer() instanceof QueryableDataContainer);
      assertTrue(c.dataContainer().<byte[]>keyEquivalence() instanceof AnyEquivalence);
      assertTrue(c.dataContainer().<byte[]>valueEquivalence() instanceof AnyEquivalence);

      if (holder.getNamedConfigurationBuilders().containsKey("store-as-binary")) {
         c = holder.getNamedConfigurationBuilders().get("store-as-binary").build();
         assertSame(StorageType.BINARY, c.memory().storageType());
      }
   }

   private static void assertTemplateConfiguration(ConfigurationBuilderHolder holder, String name) {
      Configuration configuration = holder.getNamedConfigurationBuilders().get(name).build();
      assertNotNull("Configuration " + name + " expected", configuration);
      assertTrue("Configuration " + name + " should be a template", configuration.isTemplate());
   }

   private static void assertCacheConfiguration(ConfigurationBuilderHolder holder, String name) {
      Configuration configuration = holder.getNamedConfigurationBuilders().get(name).build();
      assertNotNull("Configuration " + name + " expected", configuration);
      assertFalse("Configuration " + name + " should not be a template", configuration.isTemplate());
   }

   private static <T> T getStoreConfiguration(Configuration c, Class<T> configurationClass) {
      for (StoreConfiguration pc : c.persistence().stores()) {
         if (configurationClass.isInstance(pc)) {
            return (T) pc;
         }
      }
      throw new NoSuchElementException("There is no store of type " + configurationClass);
   }

   @Test(expectedExceptions = CacheConfigurationException.class)
   public void testUnsupportedConfiguration() throws Exception {
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.fromXml("configs/unified/old.xml", true)) {

               @Override
               public void call() {
                  fail("Parsing an unsupported file should have failed.");
               }

      });
   }

   public static final class CustomInterceptor1 extends CommandInterceptor {}
   public static final class CustomInterceptor2 extends CommandInterceptor {}
   public static final class CustomInterceptor3 extends CommandInterceptor {}
   public static final class CustomInterceptor4 extends CommandInterceptor {
      String foo; // configured via XML
   }

}
