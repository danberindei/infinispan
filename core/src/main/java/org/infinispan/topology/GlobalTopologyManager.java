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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;

import org.infinispan.distribution.newch.AdvancedConsistentHash;
import org.infinispan.distribution.newch.ConsistentHash;
import org.infinispan.distribution.newch.ConsistentHashFactory;
import org.infinispan.remoting.transport.Address;

/**
 * Trying out state transfer/topology change interfaces.
 */
public class GlobalTopologyManager {
}

class CacheTopologyManager {
   private ConsistentHashFactory chf;
   Address self;
   List<TopologyChangeHandler> topologyChangeHandlers;
   volatile CacheTopology currentTopology;
   private RebalanceTask rebalanceTask;

   public void updateTopology(CacheTopology newTopology, CacheTopology newRebalanceTopology) {
      assert newTopology != null;
      assert newTopology.topologyId == currentTopology.topologyId;

      currentTopology = newTopology;

      for (TopologyChangeHandler handler : topologyChangeHandlers) {
         handler.updateTopology(newTopology);
      }

      if (rebalanceTask != null) {
         rebalanceTask.updateTopology(newRebalanceTopology);
      }
   }

   public void rebalance(CacheTopology newRebalanceTopology) {
      assert rebalanceTask == null;

      rebalanceTask = new RebalanceTask(this, newRebalanceTopology);
      rebalanceTask.run();
   }
}

interface TopologyChangeHandler {
   void updateTopology(CacheTopology newTopology);

   void beforeRebalance(CacheTopology currentTopology, CacheTopology rebalanceTopology, boolean up);
}

class RebalanceTask {
   private final CacheTopologyManager cacheTopologyManager;
   private CacheTopology rebalanceTopology;

   public RebalanceTask(CacheTopologyManager cacheTopologyManager, CacheTopology newRebalanceTopology) {
      this.cacheTopologyManager = cacheTopologyManager;
      this.rebalanceTopology = newRebalanceTopology;
   }

   public void updateTopology(CacheTopology newRebalanceTopology) {
      rebalanceTopology = newRebalanceTopology;
   }

   public void run() {
      List<TopologyChangeHandler> handlers = cacheTopologyManager.topologyChangeHandlers;

      // before rebalance: e.g. prepare the list of transactions to push to other nodes
      for (int i = 0; i < handlers.size(); i++) {
         TopologyChangeHandler handler = handlers.get(i);
         handler.beforeRebalance(cacheTopologyManager.currentTopology, rebalanceTopology, true);
      }
      for (int i = handlers.size() - 1; i >= 0; i--) {
         TopologyChangeHandler handler = handlers.get(i);
         handler.beforeRebalance(cacheTopologyManager.currentTopology, rebalanceTopology, false);
      }

      // rebalance
      List<Integer> segmentsToPull = computeSegmentsToPull(cacheTopologyManager.currentTopology.ch,
            rebalanceTopology.ch, cacheTopologyManager.self);
      while (!segmentsToPull.isEmpty()) {
         // the current CH might change between iterations, as nodes leave the cache
         // so we can't pre-compute the pushing owners and pushed segments for each of them
         Address pusher = computePushingOwner(cacheTopologyManager.currentTopology.ch, segmentsToPull.get(0));
         Collection<Integer> pushedSegments = computePushedSegments(cacheTopologyManager.currentTopology.ch, pusher);
      }

      // after rebalance: e.g. invalidate keys that are no longer local
      for (int i = 0; i < handlers.size(); i++) {
         TopologyChangeHandler handler = handlers.get(i);
         handler.beforeRebalance(cacheTopologyManager.currentTopology, rebalanceTopology, true);
      }
      for (int i = handlers.size() - 1; i >= 0; i--) {
         TopologyChangeHandler handler = handlers.get(i);
         handler.beforeRebalance(cacheTopologyManager.currentTopology, rebalanceTopology, false);
      }
   }

   private Collection<Integer> computePushedSegments(AdvancedConsistentHash ch, Address pusher) {
      List<Integer> pushedSegments = new ArrayList<Integer>();
      for (int i = 0; i < ch.getNumSegments(); i++) {
         List<Address> owners = ch.locateOwnersForSegment(i);
         if (owners.get(owners.size() - 1).equals(pusher)) {
            pushedSegments.add(i);
         }
      }
      return pushedSegments;
   }

   private Address computePushingOwner(AdvancedConsistentHash currentCH, Integer integer) {
      List<Address> owners = currentCH.locateOwnersForSegment(integer);
      return owners.get(owners.size() - 1);
   }


   private List<Integer> computeSegmentsToPull(AdvancedConsistentHash currentCH, AdvancedConsistentHash rebalanceCH, Address self) {
      List<Integer> segmentsToPull = new ArrayList<Integer>();
      for (int i = 0; i < currentCH.getNumSegments(); i++) {
         if (rebalanceCH.locateOwnersForSegment(i).contains(self)
               && !currentCH.locateOwnersForSegment(i).contains(self)) {
            segmentsToPull.add(i);
         }
      }
      return segmentsToPull;
   }
}

class CacheTopology {
   int topologyId;
   List<Address> members;
   // no CH for replication caches
   AdvancedConsistentHash ch;
}