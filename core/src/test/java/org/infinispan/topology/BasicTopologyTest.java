/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.topology;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commons.hash.Hash;
import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.config.Configuration;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Created with
 *
 * @author Dan Berindei
 * @since 5.2
 */
@Test
public class BasicTopologyTest extends MultipleCacheManagersTest {
   @Override
   protected void createCacheManagers() throws Throwable {
      // do nothing on startup
   }

   public void test() {
      // start one node
      ConfigurationBuilder config = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      EmbeddedCacheManager manager1 = addClusterEnabledCacheManager(config);
      Cache<Object,Object> cache1 = manager1.getCache("topologyTest");
      cache1.put("k", "v");

      EmbeddedCacheManager manager2 = addClusterEnabledCacheManager(config);
      Cache<Object,Object> cache2 = manager2.getCache("topologyTest");
      log.info("Cluster formed with 2 caches");

      EmbeddedCacheManager manager3 = addClusterEnabledCacheManager(config);
      Cache<Object,Object> cache3 = manager3.getCache("topologyTest");
      log.info("Cluster formed with 3 caches");
   }

   public void testInvertHash() {
      Hash hashFunction = new MurmurHash3();
      int numSegments = 151;
      int numOwners = 2;
      double leewayFraction = 0.0001;

      long startNanos = System.nanoTime();

      int segmentSize = (int)Math.ceil((double)Integer.MAX_VALUE / numSegments);
      int leeway = (int) (leewayFraction * segmentSize);
      System.out.printf("leeway=%f%%\n", ((double)leeway*numSegments/Integer.MAX_VALUE*100));
      List[] denormalizedSegmentOwners = new List[numSegments];
      for (int i = 0; i < numSegments; i++) {
         denormalizedSegmentOwners[i] = new ArrayList<Integer>(numOwners);
      }
      int segmentsLeft = numSegments;

      // Allows overflow, if we didn't find all segments in the 0..MAX_VALUE range
      for (int i = 0; segmentsLeft != 0; i++) {
         int normalizedHash = hashFunction.hash(i) & Integer.MAX_VALUE;
         if (normalizedHash % segmentSize < leeway) {
            int segmentIdx = normalizedHash / segmentSize;
            if (denormalizedSegmentOwners[segmentIdx].size() < numOwners) {
               denormalizedSegmentOwners[segmentIdx].add(i);
               if (denormalizedSegmentOwners[segmentIdx].size() == numOwners) {
                  segmentsLeft--;
                  System.out.print('+');
               }
            }
         }
      }
      System.out.println();

      long endNanos = System.nanoTime();
      System.out.println(TimeUnit.NANOSECONDS.toMillis(endNanos - startNanos));

      System.out.println(Arrays.toString(denormalizedSegmentOwners));
   }
}
