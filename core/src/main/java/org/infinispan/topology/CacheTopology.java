package org.infinispan.topology;

import java.util.Collection;

import org.infinispan.distribution.newch.ConsistentHash;
import org.infinispan.remoting.transport.Address;

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
public class CacheTopology {
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

   public Collection<Address> getMembers() {
      if (pendingCH != null)
         return pendingCH.getMembers();
      else
         return currentCH.getMembers();
   }

   public ConsistentHash getReadConsistentHash() {
      return currentCH;
   }

   public ConsistentHash getWriteConsistentHash() {
      if (pendingCH != null)
         return pendingCH;
      else
         return currentCH;
   }
}
