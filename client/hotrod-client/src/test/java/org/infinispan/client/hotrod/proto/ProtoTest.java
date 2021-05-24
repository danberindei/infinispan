package org.infinispan.client.hotrod.proto;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * Tests various API methods of remote cache
 *
 * @author Dan Berindei
 * @since 13.0
 */
@Test(groups = "functional", testName = "client.hotrod.proto.ProtoTest")
public class ProtoTest extends MultiHotRodServersTest {

   private static final int NR_NODES = 2;
   private static final String CACHE_NAME = "api-cache";

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cacheBuilder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      createHotRodServers(NR_NODES, new ConfigurationBuilder());
      defineInAll(CACHE_NAME, cacheBuilder);
   }

   @Override
   protected org.infinispan.client.hotrod.configuration.ConfigurationBuilder createHotRodClientConfigurationBuilder(
         String host, int serverPort) {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = super
            .createHotRodClientConfigurationBuilder(host, serverPort);
      clientBuilder.forceReturnValues(false);
      clientBuilder.addContextInitializers(new GrowthContextInitializerImpl());
      return clientBuilder;
   }

   public void testListOfListOfString() {
      GrowthStats stats = new GrowthStats();
      // Arrays.ArrayList is not supported
      List<List<String>> lists = new ArrayList<>(Arrays.asList(new ArrayList<>(Arrays.asList("a", "b")), new ArrayList<>(Arrays.asList("1", "2"))));
      stats.setData(lists);

      assertNull(remoteCache().put("key", stats));
      GrowthStats stats2 = (GrowthStats) remoteCache().get("key");
      assertNotNull(stats2);
      assertEquals(lists, stats2.getData());
   }

   private RemoteCache<Object, Object> remoteCache() {
      return client(0).getCache(CACHE_NAME);
   }
}
