package org.infinispan.statetransfer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import org.infinispan.commons.util.SmallIntSet;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.CacheTopology;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import net.jcip.annotations.GuardedBy;

/**
 * @author Dan Berindei
 * @since 9.1
 */
public class StateTransfer {
   private static final Log log = LogFactory.getLog(StateTransfer.class);
   private static final boolean trace = log.isTraceEnabled();

   public static final StateTransfer NONE = new StateTransfer();

   public final int rebalanceId;

   @GuardedBy("transferMapsLock")
   int topologyId;

   @GuardedBy("transferMapsLock")
   public CacheTopology cacheTopology;

   /**
    * Indicates if there is a rebalance in progress and there the local node has not yet received
    * all the new segments yet. It is set to true when rebalance starts and becomes when all inbound transfers have
    * completed
    * (before stateTransferTopologyId is set back to NO_REBALANCE_IN_PROGRESS).
    */
   public final AtomicBoolean waitingForState = new AtomicBoolean(false);

   public CompletableFuture<Void> stateTransferFuture = new CompletableFuture<>();

   public final Object transferMapsLock = new Object();

   /**
    * A map that keeps track of current inbound state transfers by source address. There could be multiple transfers
    * flowing in from the same source (but for different segments) so the values are lists. This works in tandem with
    * transfersBySegment so they always need to be kept in sync and updates to both of them need to be atomic.
    */
   @GuardedBy("transferMapsLock")
   public final Map<Address, List<InboundTransferTask>> transfersBySource = new HashMap<>();

   /**
    * A map that keeps track of current inbound state transfers by segment id. There is at most one transfers per
    * segment.
    * This works in tandem with transfersBySource so they always need to be kept in sync and updates to both of them
    * need to be atomic.
    */
   @GuardedBy("transferMapsLock")
   public final Map<Integer, List<InboundTransferTask>> transfersBySegment = new HashMap<>();

   private StateTransfer() {
      // Only used for NONE
      topologyId = -1;
      rebalanceId = -1;
      stateTransferFuture.complete(null);
   }

   public StateTransfer(CacheTopology cacheTopology) {
      this.cacheTopology = cacheTopology;
      this.topologyId = cacheTopology.getTopologyId();
      this.rebalanceId = cacheTopology.getRebalanceId();
      if (trace)
         log.tracef("Initializing rebalance %d (initial topology id %d)", rebalanceId, topologyId);
   }

   @GuardedBy("transferMapsLock")
   public void addTransfer(InboundTransferTask inboundTransfer, Set<Integer> segments) {
      for (int segmentId : segments) {
         transfersBySegment.computeIfAbsent(segmentId, s -> new ArrayList<>())
                           .add(inboundTransfer);
      }
      transfersBySource.computeIfAbsent(inboundTransfer.getSource(), s -> new ArrayList<>())
                       .add(inboundTransfer);
   }

   public boolean removeTransfer(InboundTransferTask inboundTransfer, StateConsumerImpl stateConsumer) {
      boolean found = false;
      synchronized (transferMapsLock) {
         if (trace)
            log.tracef("Removing inbound transfers from node %s for segments %s",
                       inboundTransfer.getSegments(), inboundTransfer.getSource(), stateConsumer.cacheName);
         List<InboundTransferTask> transfers = transfersBySource.get(inboundTransfer.getSource());
         if (transfers != null && (found = transfers.remove(inboundTransfer)) && transfers.isEmpty()) {
            transfersBySource.remove(inboundTransfer.getSource());
         }
         for (int segment : inboundTransfer.getSegments()) {
            transfers = transfersBySegment.get(segment);
            if (transfers != null && transfers.remove(inboundTransfer) && transfers.isEmpty()) {
               transfersBySegment.remove(segment);
            }
         }
      }
      return found;
   }

   public void addTransfer(Address source, Set<Integer> segmentsFromSource, InboundTransferTask inboundTransfer) {
      synchronized (transferMapsLock) {
         if (trace) {
            log.tracef("Adding transfer from %s for segments %s for topology %d", source, segmentsFromSource,
                       topologyId);
         }
         if (segmentsFromSource.stream().anyMatch(s -> transfersBySegment.keySet().contains(s))) {
            throw new IllegalStateException("Trying to request a segment that is already in progress");
         }

         addTransfer(inboundTransfer, segmentsFromSource);
      }
   }

   public Set<Integer> activeSegments() {
      synchronized (transferMapsLock) {
         return new SmallIntSet(transfersBySegment.keySet());
      }
   }
}
