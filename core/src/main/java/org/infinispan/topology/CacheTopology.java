package org.infinispan.topology;

import org.infinispan.distribution.newch.ConsistentHash;

/**
 * The status of a cache from a distribution/state transfer point of view.
 * <p/>
 * The pending CH can be {@code null} if we don't have a state transfer in progress.
 * <p/>
 * The {@code topologyId} is incremented every time the topology changes (i.e. state transfer
 * starts or ends). It is not modified when the consistent hashes are updated without requiring state
 * transfer (e.g. when a member leaves).
 *
 * @author Dan Berindei
 * @since 5.2
 */
class CacheTopology {
   int topologyId;
   ConsistentHash currentCH;
   ConsistentHash pendingCH;

   CacheTopology(int topologyId, ConsistentHash currentCH, ConsistentHash pendingCH) {
      this.topologyId = topologyId;
      this.currentCH = currentCH;
      this.pendingCH = pendingCH;
   }

   public int getTopologyId() {
      return topologyId;
   }

   public ConsistentHash getCurrentCH() {
      return currentCH;
   }

   public ConsistentHash getPendingCH() {
      return pendingCH;
   }
}
