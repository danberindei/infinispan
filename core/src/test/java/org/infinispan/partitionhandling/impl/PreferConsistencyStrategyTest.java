package org.infinispan.partitionhandling.impl;

import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.distribution.TestAddress;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.ConsistentHashFactory;
import org.infinispan.distribution.ch.impl.DefaultConsistentHashFactory;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.topology.CacheJoinInfo;
import org.infinispan.topology.CacheStatusResponse;
import org.infinispan.topology.CacheTopology;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.infinispan.distribution.TestAddress.A;
import static org.infinispan.distribution.TestAddress.B;
import static org.infinispan.distribution.TestAddress.C;
import static org.infinispan.distribution.TestAddress.D;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

/**
 * @author Dan Berindei
 * @since 8.1
 */
@Test(groups = "unit", testName = "partitionhandling.PreferConsistencyStrategyTest")
public class PreferConsistencyStrategyTest extends AbstractInfinispanTest {
   public void testOnPartitionMerge() throws Exception {
      // 17:22:04,638 DEBUG (transport-thread-NodeS-p40708-t3:[]) [ClusterTopologyManagerImpl] Updating
      // cluster-wide stable topology for cache ___defaultcache, topology = CacheTopology{id=6,
      // rebalanceId=3, currentCH=DefaultConsistentHash{ns=60, owners = (4)[NodeQ-11175: 15+15,
      // NodeR-23094: 15+15, NodeS-5998: 15+15, NodeT-13783: 15+15]}, pendingCH=null, unionCH=null,
      // actualMembers=[NodeQ-11175, NodeR-23094, NodeS-5998, NodeT-13783]}
      // 17:22:07,081 DEBUG (transport-thread-NodeS-p40708-t6:[]) [ClusterTopologyManagerImpl] Got 3 status
      // responses. members are [NodeS-5998, NodeQ-11175, NodeR-23094, NodeT-13783]
      // 17:22:07,081 DEBUG (transport-thread-NodeS-p40708-t1:[]) [ClusterCacheStatus] Recovered 2
      // partition(s) for cache ___defaultcache: [CacheTopology{id=8, rebalanceId=3,
      // currentCH=DefaultConsistentHash{ns=60, owners = (4)[NodeQ-11175: 15+15, NodeR-23094: 15+15,
      // NodeS-5998: 15+15, NodeT-13783: 15+15]}, pendingCH=null, unionCH=null,
      // actualMembers=[NodeS-5998]}, CacheTopology{id=10, rebalanceId=4,
      // currentCH=DefaultConsistentHash{ns=60, owners = (3)[NodeQ-11175: 20+20, NodeR-23094: 20+20,
      // NodeT-13783: 20+20]}, pendingCH=null, unionCH=null, actualMembers=[NodeQ-11175, NodeR-23094,
      // NodeT-13783]}]
      ConsistentHashFactory chFactory = new DefaultConsistentHashFactory();
      List<Address> initialMembers = Arrays.asList(A, B, C, D);
      MurmurHash3 hashFunction = MurmurHash3.getInstance();
      ConsistentHash initialCH = chFactory.create(hashFunction, 2, 60, initialMembers, null);
      List<Address> majorityMembers = Arrays.asList(A, C, D);
      ConsistentHash majorityCH = chFactory.updateMembers(initialCH, majorityMembers, null);
      CacheJoinInfo joinInfo = new CacheJoinInfo(chFactory, hashFunction, 60, 2, 60000, false, true, 1);
      CacheTopology majorityTopology = new CacheTopology(10, 4, majorityCH, null, majorityMembers);
      CacheStatusResponse majorityStatus =
            new CacheStatusResponse(joinInfo, majorityTopology, majorityTopology, AvailabilityMode.AVAILABLE);
      List<Address> minorityMembers = Collections.singletonList(B);
      CacheTopology minorityTopology = new CacheTopology(8, 3, initialCH, null, minorityMembers);
      CacheTopology initialTopology = new CacheTopology(6, 3, initialCH, null, initialMembers);
      CacheStatusResponse status2 =
            new CacheStatusResponse(joinInfo, minorityTopology, initialTopology, AvailabilityMode.DEGRADED_MODE);
      Collection<CacheStatusResponse> responses = Arrays.asList(
            majorityStatus, status2, majorityStatus);

      AvailabilityStrategyContext context = mock(AvailabilityStrategyContext.class);
      when(context.getExpectedMembers()).thenReturn(Arrays.asList(A, B, C, D));


      PreferConsistencyStrategy strategy = new PreferConsistencyStrategy();
      strategy.onPartitionMerge(context, responses);
      Mockito.verifyNoMoreInteractions(context);
   }
}