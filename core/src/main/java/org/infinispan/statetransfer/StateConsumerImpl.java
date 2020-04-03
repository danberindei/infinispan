package org.infinispan.statetransfer;

import static org.infinispan.context.Flag.CACHE_MODE_LOCAL;
import static org.infinispan.context.Flag.IGNORE_RETURN_VALUES;
import static org.infinispan.context.Flag.IRAC_STATE;
import static org.infinispan.context.Flag.PUT_FOR_STATE_TRANSFER;
import static org.infinispan.context.Flag.SKIP_LOCKING;
import static org.infinispan.context.Flag.SKIP_OWNERSHIP_CHECK;
import static org.infinispan.context.Flag.SKIP_REMOTE_LOOKUP;
import static org.infinispan.context.Flag.SKIP_SHARED_CACHE_STORE;
import static org.infinispan.context.Flag.SKIP_XSITE_BACKUP;
import static org.infinispan.factories.KnownComponentNames.NON_BLOCKING_EXECUTOR;
import static org.infinispan.persistence.manager.PersistenceManager.AccessMode.PRIVATE;
import static org.infinispan.util.concurrent.CompletionStages.handleAndCompose;
import static org.infinispan.util.concurrent.CompletionStages.ignoreValue;
import static org.infinispan.util.logging.Log.CLUSTER;
import static org.infinispan.util.logging.Log.PERSISTENCE;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commons.IllegalLifecycleStateException;
import org.infinispan.commons.tx.TransactionImpl;
import org.infinispan.commons.tx.XidImpl;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.conflict.impl.InternalConflictManager;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.TriangleOrderManager;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.metadata.impl.InternalMetadataImpl;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachelistener.annotation.DataRehashed;
import org.infinispan.notifications.cachelistener.cluster.ClusterListenerReplicateCallable;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.reactive.publisher.impl.LocalPublisherManager;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.responses.ValidResponse;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.impl.PassthroughSingleResponseCollector;
import org.infinispan.remoting.transport.impl.SingleResponseCollector;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.impl.RemoteTransaction;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.CommandAckCollector;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import net.jcip.annotations.GuardedBy;

/**
 * {@link StateConsumer} implementation.
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
@Scope(Scopes.NAMED_CACHE)
public class StateConsumerImpl implements StateConsumer {
   private static final Log log = LogFactory.getLog(StateConsumerImpl.class);
   private static final boolean trace = log.isTraceEnabled();
   protected static final int NO_STATE_TRANSFER_IN_PROGRESS = -1;
   protected static final long STATE_TRANSFER_FLAGS = EnumUtil.bitSetOf(PUT_FOR_STATE_TRANSFER, CACHE_MODE_LOCAL,
                                                                        IGNORE_RETURN_VALUES, SKIP_REMOTE_LOOKUP,
                                                                        SKIP_SHARED_CACHE_STORE, SKIP_OWNERSHIP_CHECK,
                                                                        SKIP_XSITE_BACKUP, SKIP_LOCKING, IRAC_STATE);
   public static final String NO_KEY = "N/A";

   @Inject protected ComponentRef<Cache<Object, Object>> cache;
   @Inject protected LocalTopologyManager localTopologyManager;
   @Inject protected Configuration configuration;
   @Inject protected RpcManager rpcManager;
   @Inject protected TransactionManager transactionManager;   // optional
   @Inject protected CommandsFactory commandsFactory;
   @Inject protected TransactionTable transactionTable;       // optional
   @Inject protected InternalDataContainer<Object, Object> dataContainer;
   @Inject protected PersistenceManager persistenceManager;
   @Inject protected AsyncInterceptorChain interceptorChain;
   @Inject protected InvocationContextFactory icf;
   @Inject protected StateTransferLock stateTransferLock;
   @Inject protected CacheNotifier<?, ?> cacheNotifier;
   @Inject protected CommitManager commitManager;
   @Inject @ComponentName(NON_BLOCKING_EXECUTOR)
   protected Executor nonBlockingExecutor;
   @Inject protected CommandAckCollector commandAckCollector;
   @Inject protected TriangleOrderManager triangleOrderManager;
   @Inject protected DistributionManager distributionManager;
   @Inject protected KeyPartitioner keyPartitioner;
   @Inject protected InternalConflictManager<?, ?> conflictManager;
   @Inject protected LocalPublisherManager<Object, Object> localPublisherManager;
   @Inject PerCacheInboundInvocationHandler inboundInvocationHandler;

   protected String cacheName;
   protected long timeout;
   protected boolean isFetchEnabled;
   protected boolean isTransactional;
   protected boolean isInvalidationMode;
   protected volatile KeyInvalidationListener keyInvalidationListener; //for test purpose only!

   protected volatile CacheTopology cacheTopology;

   /**
    * Indicates if there is a state transfer in progress. It is set to the new topology id when onTopologyUpdate with
    * isRebalance==true is called.
    * It is changed back to NO_REBALANCE_IN_PROGRESS when a topology update with a null pending CH is received.
    */
   protected final AtomicInteger stateTransferTopologyId = new AtomicInteger(NO_STATE_TRANSFER_IN_PROGRESS);

   /**
    * Indicates if there is a rebalance in progress and there the local node has not yet received
    * all the new segments yet. It is set to true when rebalance starts and becomes when all inbound transfers have
    * completed
    * (before stateTransferTopologyId is set back to NO_REBALANCE_IN_PROGRESS).
    */
   protected final AtomicBoolean waitingForState = new AtomicBoolean(false);
   protected CompletableFuture<Void> stateTransferFuture = CompletableFutures.completedNull();

   protected final Object transfersLock = new Object();

   /**
    * The set of segments that have transactions but not yet data.
    *
    * <p>Includes the segments in the current {@code inboundTransfer},
    * but not the failed segments or the received segments.</p>
    */
   @GuardedBy("transfersLock")
   protected final IntSet pendingSegments = IntSets.mutableEmptySet();

   /**
    * The set of segments that have been received but are still not owned in the current CH.
    */
   @GuardedBy("transfersLock")
   protected final IntSet receivedSegments = IntSets.mutableEmptySet();

   /**
    * The set of segments that were not successfully requested, and will be retried in the next topology.
    */
   @GuardedBy("transfersLock")
   protected final IntSet failedSegments = IntSets.mutableEmptySet();

   /**
    * The current inbound state task.
    */
   @GuardedBy("transfersLock")
   protected InboundTransferTask inboundTransfer;

   private volatile boolean ownsData = false;

   // Use the state transfer timeout for RPCs instead of the regular remote timeout
   protected RpcOptions rpcOptions;
   protected volatile boolean running;

   public StateConsumerImpl() {
   }

   /**
    * Stops applying incoming state. Also stops tracking updated keys. Should be called at the end of state transfer or
    * when a ClearCommand is committed during state transfer.
    */
   @Override
   public void stopApplyingState(int topologyId) {
      if (trace) log.tracef("Stop keeping track of changed keys for state transfer in topology %d", topologyId);
      commitManager.stopTrack(PUT_FOR_STATE_TRANSFER);
   }

   public boolean hasPendingSegments() {
      synchronized (transfersLock) {
         return !pendingSegments.isEmpty();
      }
   }

   @Override
   public boolean isStateTransferInProgress() {
      return stateTransferTopologyId.get() != NO_STATE_TRANSFER_IN_PROGRESS;
   }

   @Override
   public boolean isStateTransferInProgressForKey(Object key) {
      if (isInvalidationMode) {
         // In invalidation mode it is of not much relevance if the key is actually being transferred right now.
         // A false response to this will just mean the usual remote lookup before a write operation is not
         // performed and a null is assumed. But in invalidation mode the user must expect the data can disappear
         // from cache at any time so this null previous value should not cause any trouble.
         return false;
      }

      DistributionInfo distributionInfo = distributionManager.getCacheTopology().getDistribution(key);
      return distributionInfo.isWriteOwner() && !distributionInfo.isReadOwner();
   }

   @Override
   public boolean ownsData() {
      return ownsData;
   }

   @Override
   public CompletionStage<CompletionStage<Void>> onTopologyUpdate(final CacheTopology cacheTopology,
                                                                  final boolean isRebalance) {
      ConsistentHash newReadCh = cacheTopology.getReadConsistentHash();
      ConsistentHash newWriteCh = cacheTopology.getWriteConsistentHash();
      CacheTopology previousCacheTopology = this.cacheTopology;
      ConsistentHash previousReadCh =
            previousCacheTopology != null ? previousCacheTopology.getCurrentCH() : null;
      ConsistentHash previousWriteCh =
            previousCacheTopology != null ? previousCacheTopology.getWriteConsistentHash() : null;
      IntSet newWriteSegments = getOwnedSegments(newWriteCh);
      IntSet previousWriteSegments = getOwnedSegments(previousWriteCh);
      IntSet newReadSegments = getOwnedSegments(newReadCh);

      final boolean isMember = cacheTopology.getMembers().contains(rpcManager.getAddress());
      final boolean wasMember = previousWriteCh != null &&
                                previousWriteCh.getMembers().contains(rpcManager.getAddress());

      if (trace)
         log.tracef("Received new topology for cache %s, isRebalance = %b, isMember = %b, topology = %s", cacheName,
                    isRebalance, isMember, cacheTopology);

      if (!ownsData && isMember) {
         ownsData = true;
      } else if (ownsData && !isMember) {
         // This can happen after a merge, if the local node was in a minority partition.
         ownsData = false;
      }

      // If a member leaves/crashes immediately after a rebalance was started, the new CH_UPDATE
      // command may be executed before the REBALANCE_START command, so it has to start the rebalance.
      boolean addedPendingCH = cacheTopology.getPendingCH() != null && wasMember &&
                               previousCacheTopology.getPendingCH() == null;
      boolean startConflictResolution =
            !isRebalance && cacheTopology.getPhase() == CacheTopology.Phase.CONFLICT_RESOLUTION;
      boolean startRebalance = isRebalance || (addedPendingCH && !startConflictResolution);
      if (startRebalance && !isRebalance) {
         if (trace) log.tracef("Forcing startRebalance = true");
      }
      CompletionStage<Void> stage = CompletableFutures.completedNull();
      if (startRebalance) {
         // Only update the rebalance topology id when starting the rebalance, as we're going to ignore any state
         // response with a smaller topology id
         stateTransferTopologyId.compareAndSet(NO_STATE_TRANSFER_IN_PROGRESS, cacheTopology.getTopologyId());
         conflictManager.cancelVersionRequests();
         if (cacheNotifier.hasListener(DataRehashed.class)) {
            stage = cacheNotifier.notifyDataRehashed(cacheTopology.getCurrentCH(), cacheTopology.getPendingCH(),
                                                    cacheTopology.getUnionCH(), cacheTopology.getTopologyId(), true);
         }
      }
      stage = stage.thenCompose(ignored -> {
         if (startConflictResolution) {
            // This stops state being applied from a prior rebalance and also prevents tracking from being stopped
            stateTransferTopologyId.set(NO_STATE_TRANSFER_IN_PROGRESS);
         }

         // Make sure we don't send a REBALANCE_CONFIRM command before we've added all the transfer tasks
         // even if some of the tasks are removed and re-added
         waitingForState.set(false);
         stateTransferFuture = new CompletableFuture<>();

         beforeTopologyInstalled(cacheTopology.getTopologyId(), startRebalance, previousWriteCh, newWriteCh);

         if (!configuration.clustering().cacheMode().isInvalidation()) {
            // Owned segments
            dataContainer.addSegments(newWriteSegments);
            // TODO Should we throw an exception if addSegments() returns false?
            return ignoreValue(persistenceManager.addSegments(newWriteSegments));
         }
         return CompletableFutures.completedNull();
      });
      stage = stage.thenCompose(ignored -> {
         // We need to track changes so that user puts during conflict resolution are prioritised over
         // state transfer or conflict resolution updates
         // Tracking is stopped once the subsequent rebalance completes
         if (startRebalance || startConflictResolution) {
            if (trace) log.tracef("Start keeping track of keys for rebalance");
            commitManager.stopTrack(PUT_FOR_STATE_TRANSFER);
            commitManager.startTrack(PUT_FOR_STATE_TRANSFER);
         }

         // Ensures writes to the data container use the right consistent hash
         // Writers block on the state transfer shared lock, so we keep the exclusive lock as short as possible
         stateTransferLock.acquireExclusiveTopologyLock();
         try {
            this.cacheTopology = cacheTopology;
            distributionManager.setCacheTopology(cacheTopology);
         } finally {
            stateTransferLock.releaseExclusiveTopologyLock();
         }

         stateTransferLock.notifyTopologyInstalled(cacheTopology.getTopologyId());
         inboundInvocationHandler.checkForReadyTasks();

         if (!wasMember && isMember) {
            return fetchClusterListeners(cacheTopology);
         }

         return CompletableFutures.completedNull();
      });
      stage = stage.thenCompose(ignored -> {
         // fetch transactions and data segments from other owners if this is enabled
         if (startConflictResolution || (!isTransactional && !isFetchEnabled)) {
            return CompletableFutures.completedNull();
         }
         IntSet addedSegments, removedSegments;
         if (previousWriteCh == null) {
            // If we have any segments assigned in the initial CH, it means we are the first member.
            // If we are not the first member, we can only add segments via rebalance.
            removedSegments = IntSets.immutableEmptySet();
            addedSegments = IntSets.immutableEmptySet();

            if (trace) {
               log.tracef("On cache %s we have: added segments: %s", cacheName, addedSegments);
            }
         } else {
            if (newWriteSegments.size() == newWriteCh.getNumSegments()) {
               // Optimization for replicated caches
               removedSegments = IntSets.immutableEmptySet();
            } else {
               removedSegments = IntSets.mutableCopyFrom(previousWriteSegments);
               removedSegments.removeAll(newWriteSegments);
            }

            // This is a rebalance, we need to request the segments we own in the new CH.
            addedSegments = IntSets.mutableCopyFrom(newWriteSegments);
            addedSegments.removeAll(previousWriteSegments);

            if (trace) {
               log.tracef("On cache %s we have: new segments: %s; old segments: %s", cacheName, newWriteSegments,
                          previousWriteSegments);
               log.tracef("On cache %s we have: added segments: %s; removed segments: %s", cacheName,
                          addedSegments, removedSegments);
            }

            // remove inbound transfers for segments we no longer own
            cancelTransfers(removedSegments);

            // Scattered cache gets added segments on the first CH_UPDATE, and we want to keep these
            if (!startRebalance && !addedSegments.isEmpty() &&
                !configuration.clustering().cacheMode().isScattered()) {
               // If the last owner of a segment leaves the cluster, a new set of owners is assigned,
               // but the new owners should not try to retrieve the segment from each other.
               // If this happens during a rebalance, we might have already sent our rebalance
               // confirmation, so the coordinator won't wait for us to retrieve those segments anyway.
               log.debugf("Not requesting segments %s because the last owner left the cluster",
                          addedSegments);
               addedSegments.clear();
            }

            // check if any of the existing transfers should be restarted from a different source because
            // the initial source is no longer a member
            restartBrokenTransfers(cacheTopology);
         }

         return handleSegments(startRebalance, addedSegments, removedSegments);
      });
      stage = stage.thenCompose(ignored -> {
         int stateTransferTopologyId = this.stateTransferTopologyId.get();
         if (trace)
            log.tracef("Topology update processed, stateTransferTopologyId = %d, startRebalance = %s, pending CH = %s",
                       (Object) stateTransferTopologyId, startRebalance, cacheTopology.getPendingCH());
         if (stateTransferTopologyId != NO_STATE_TRANSFER_IN_PROGRESS && !startRebalance &&
             !cacheTopology.getPhase().isRebalance()) {
            // we have received a topology update without a pending CH, signalling the end of the rebalance
            boolean changed = this.stateTransferTopologyId.compareAndSet(stateTransferTopologyId,
                                                                         NO_STATE_TRANSFER_IN_PROGRESS);
            if (changed) {
               stopApplyingState(stateTransferTopologyId);

               // if the coordinator changed, we might get two concurrent topology updates,
               // but we only want to notify the @DataRehashed listeners once
               ConsistentHash nextConsistentHash = cacheTopology.getPendingCH();
               if (nextConsistentHash == null) {
                  nextConsistentHash = cacheTopology.getCurrentCH();
               }

               if (cacheNotifier.hasListener(DataRehashed.class)) {
                  return cacheNotifier.notifyDataRehashed(previousReadCh, nextConsistentHash, previousWriteCh,
                                                          cacheTopology.getTopologyId(), false);
               }
            }
         }
         return CompletableFutures.completedNull();
      });
      return handleAndCompose(stage, (ignored, throwable) -> {
         if (trace) {
            log.tracef("Unlock State Transfer in Progress for topology ID %s", cacheTopology.getTopologyId());
         }
         stateTransferLock.notifyTransactionDataReceived(cacheTopology.getTopologyId());
         inboundInvocationHandler.checkForReadyTasks();

         // Only set the flag here, after all the transfers have been added to the transfersBySource map
         if (stateTransferTopologyId.get() != NO_STATE_TRANSFER_IN_PROGRESS && isMember) {
            waitingForState.set(true);
         }

         notifyEndOfStateTransferIfNeeded();

         // Remove the transactions whose originators have left the cache.
         // Need to do it now, after we have applied any transactions from other nodes,
         // and after notifyTransactionDataReceived - otherwise the RollbackCommands would block.
         try {
            if (transactionTable != null) {
               transactionTable.cleanupLeaverTransactions(rpcManager.getTransport().getMembers());
            }
         } catch (Exception e) {
            // Do not fail state transfer when the cleanup fails. See ISPN-7437 for details.
            log.transactionCleanupError(e);
         }

         commandAckCollector.onMembersChange(newWriteCh.getMembers());

         // Receiving state (READ_OLD_WRITE_ALL/TRANSITORY) is completed through notifyEndOfStateTransferIfNeeded
         // and STABLE does not have to be confirmed at all
         switch (cacheTopology.getPhase()) {
            case READ_ALL_WRITE_ALL:
            case READ_NEW_WRITE_ALL:
               stateTransferFuture.complete(null);
         }

         // Any data for segments we do not own should be removed from data container and cache store
         // We need to discard data from all segments we don't own, not just those we previously owned,
         // when we lose membership (e.g. because there was a merge, the local partition was in degraded mode
         // and the other partition was available) or when L1 is enabled.
         if ((isMember || wasMember) && cacheTopology.getPhase() == CacheTopology.Phase.NO_REBALANCE) {
            int numSegments = newWriteCh.getNumSegments();
            IntSet removedSegments = IntSets.mutableCopyFrom(IntSets.immutableRangeSet(numSegments));
            removedSegments.removeAll(newWriteSegments);

            return removeStaleData(removedSegments)
                  .thenApply(ignored1 -> {
                     conflictManager.restartVersionRequests();

                     // rethrow the original exception, if any
                     CompletableFutures.rethrowExceptionIfPresent(throwable);
                     return stateTransferFuture;
                  });
         }

         CompletableFutures.rethrowExceptionIfPresent(throwable);
         return CompletableFuture.completedFuture(stateTransferFuture);
      });
   }

   private CompletionStage<Void> fetchClusterListeners(CacheTopology cacheTopology) {
      if (!configuration.clustering().cacheMode().isDistributed() &&
          !configuration.clustering().cacheMode().isScattered()) {
         return CompletableFutures.completedNull();
      }

      return getClusterListeners(cacheTopology.getTopologyId(), cacheTopology.getMembers()).thenAccept(callables -> {
         Cache<Object, Object> cache = this.cache.wired();
         for (ClusterListenerReplicateCallable<Object, Object> callable : callables) {
            try {
               // TODO: need security check?
               // We have to invoke a separate method as we can't retrieve the cache as it is still starting
               callable.accept(cache.getCacheManager(), cache);
            } catch (Exception e) {
               log.clusterListenerInstallationFailure(e);
            }
         }
      });
   }

   protected void beforeTopologyInstalled(int topologyId, boolean startRebalance, ConsistentHash previousWriteCh,
                                          ConsistentHash newWriteCh) {
   }

   protected CompletionStage<Void> handleSegments(boolean startRebalance, IntSet addedSegments,
                                                  IntSet removedSegments) {
      IntSet segmentsWithoutSource = IntSets.mutableCopyFrom(addedSegments);
      IntSet newReadSegments = getOwnedSegments(cacheTopology.getReadConsistentHash());
      segmentsWithoutSource.retainAll(newReadSegments);
      if (!segmentsWithoutSource.isEmpty()) {
         // The warning doesn't make sense for scattered caches, where the single owner leaving is not a problem
         log.noLiveOwnersFoundForSegments(segmentsWithoutSource, cacheName);
         addedSegments.removeAll(segmentsWithoutSource);
      }

      if (addedSegments.isEmpty()) {
         return CompletableFutures.completedNull();
      }

      CompletionStage<Void> stage = CompletableFutures.completedNull();
      if (isTransactional) {
         stage = stage.thenCompose(ignored -> requestTransactions(addedSegments));
      }

      if (isFetchEnabled) {
         stage = stage.thenRun(() -> requestSegments(addedSegments));
      }

      return stage;
   }

   protected boolean notifyEndOfStateTransferIfNeeded() {
      if (waitingForState.get()) {
         if (hasPendingSegments()) {
            if (trace)
               log.tracef("No end of state transfer notification, segments still pending");
            return false;
         }
         if (waitingForState.compareAndSet(true, false)) {
            int topologyId = stateTransferTopologyId.get();
            log.debugf("Finished receiving of segments for cache %s for topology %d.", cacheName, topologyId);
            stopApplyingState(topologyId);
            stateTransferFuture.complete(null);
         }
         if (trace)
            log.tracef("No end of state transfer notification, waitingForState already set to false by another thread");
         return false;
      }
      if (trace)
         log.tracef("No end of state transfer notification, waitingForState already set to false by another thread");
      return true;
   }

   protected IntSet getOwnedSegments(ConsistentHash consistentHash) {
      Address address = rpcManager.getAddress();
      if (consistentHash == null || !consistentHash.getMembers().contains(address)) {
         return IntSets.immutableEmptySet();
      }
      return IntSets.mutableFrom(consistentHash.getSegmentsForOwner(address));
   }

   @Override
   public CompletionStage<?> applyState(final Address sender, int topologyId, boolean pushTransfer,
                                        Collection<StateChunk> stateChunks) {
      ConsistentHash wCh = cacheTopology.getWriteConsistentHash();
      // Ignore responses received after we are no longer a member
      if (!wCh.getMembers().contains(rpcManager.getAddress())) {
         if (trace) {
            log.tracef("Ignoring received state because we are no longer a member of cache %s", cacheName);
         }
         return CompletableFutures.completedNull();
      }

      // Ignore segments that we requested for a previous rebalance
      // Can happen when the coordinator leaves, and the new coordinator cancels the rebalance in progress
      int rebalanceTopologyId = stateTransferTopologyId.get();
      if (rebalanceTopologyId == NO_STATE_TRANSFER_IN_PROGRESS && !pushTransfer) {
         log.debugf("Discarding state response with topology id %d for cache %s, we don't have a state transfer in progress",
               topologyId, cacheName);
         return CompletableFutures.completedNull();
      }
      if (topologyId < rebalanceTopologyId) {
         log.debugf("Discarding state response with old topology id %d for cache %s, state transfer request topology was %b",
               topologyId, cacheName, waitingForState);
         return CompletableFutures.completedNull();
      }

      if (trace) {
         log.tracef("Before applying the received state the data container of cache %s has %d keys", cacheName,
                    dataContainer.sizeIncludingExpired());
      }
      IntSet mySegments = IntSets.from(wCh.getSegmentsForOwner(rpcManager.getAddress()));
      Iterator<StateChunk> iterator = stateChunks.iterator();
      return applyStateIteration(sender, pushTransfer, mySegments, iterator).whenComplete((v, t) -> {
         if (trace) {
            log.tracef("After applying the received state the data container of cache %s has %d keys", cacheName,
                       dataContainer.sizeIncludingExpired());
            synchronized (transfersLock) {
               IntSet notReceived = IntSets.mutableCopyFrom(pendingSegments);
               notReceived.removeAll(receivedSegments);
               log.tracef("Segments not received yet for cache %s: %s", cacheName, notReceived);
            }
         }
      });
   }

   private CompletionStage<?> applyStateIteration(Address sender, boolean pushTransfer, IntSet mySegments,
                                                  Iterator<StateChunk> iterator) {
      CompletionStage<?> chunkStage = CompletableFutures.completedNull();
      // Replace recursion with iteration if the state was applied synchronously
      while (iterator.hasNext() && CompletionStages.isCompletedSuccessfully(chunkStage)) {
         StateChunk stateChunk = iterator.next();
         if (pushTransfer) {
            // push-transfer is specific for scattered cache but this is the easiest way to integrate it
            chunkStage = doApplyState(sender, stateChunk.getSegmentId(), stateChunk.getCacheEntries());
         } else {
            chunkStage = applyChunk(sender, mySegments, stateChunk);
         }
      }
      if (!iterator.hasNext())
         return chunkStage;

      return chunkStage.thenCompose(v -> applyStateIteration(sender, pushTransfer, mySegments, iterator));
   }

   private CompletionStage<Void> applyChunk(Address sender, IntSet mySegments, StateChunk stateChunk) {
      // Notify the inbound task that a chunk of cache entries was received
      InboundTransferTask inboundTransfer;
      synchronized (transfersLock) {
         inboundTransfer = this.inboundTransfer;

         if (inboundTransfer == null || !inboundTransfer.getSource().equals(sender) ||
             !inboundTransfer.getSegments().contains(stateChunk.getSegmentId())) {
            if (cache.wired().getStatus().allowInvocations()) {
               log.ignoringUnsolicitedState(sender, stateChunk.getSegmentId(), cacheName);
            }
            return CompletableFutures.completedNull();
         }
      }
      return doApplyState(sender, stateChunk.getSegmentId(), stateChunk.getCacheEntries())
            .thenRun(() -> inboundTransfer.onStateReceived(stateChunk.getSegmentId(), stateChunk.isLastChunk()));
   }

   private CompletionStage<?> doApplyState(Address sender, int segmentId,
                                           Collection<InternalCacheEntry<?, ?>> cacheEntries) {
      if (cacheEntries == null || cacheEntries.isEmpty())
         return CompletableFutures.completedNull();

      if (trace) log.tracef(
            "Applying new state chunk for segment %d of cache %s from node %s: received %d cache entries",
            segmentId, cacheName, sender, cacheEntries.size());

      // CACHE_MODE_LOCAL avoids handling by StateTransferInterceptor and any potential locks in StateTransferLock
      boolean transactional = transactionManager != null;
      if (transactional) {
         Object key = NO_KEY;
         Transaction transaction = new ApplyStateTransaction();
         InvocationContext ctx = icf.createInvocationContext(transaction, false);
         LocalTransaction localTransaction = ((LocalTxInvocationContext) ctx).getCacheTransaction();
         try {
            localTransaction.setStateTransferFlag(PUT_FOR_STATE_TRANSFER);
            for (InternalCacheEntry<?, ?> e : cacheEntries) {
               // CallInterceptor will preserve the timestamps if the metadata is an InternalMetadataImpl instance
               key = e.getKey();
               CompletableFuture<?> future = invokePut(segmentId, ctx, e);
               if (!future.isDone()) {
                  throw new IllegalStateException("State transfer in-tx put should always be synchronous");
               }
            }
         } catch (Throwable t) {
            logApplyException(t, key);
            return invokeRollback(localTransaction).handle((rv, t1) -> {
               transactionTable.removeLocalTransaction(localTransaction);
               if (t1 != null) {
                  t.addSuppressed(t1);
               }
               return null;
            });
         }

         return invoke1PCPrepare(localTransaction).whenComplete((rv, t) -> {
            transactionTable.removeLocalTransaction(localTransaction);
            if (t != null) {
               logApplyException(t, NO_KEY);
            }
         });
      } else {
         // non-tx cache
         AggregateCompletionStage<Void> aggregateStage = CompletionStages.aggregateCompletionStage();
         for (InternalCacheEntry<?, ?> e : cacheEntries) {
            InvocationContext ctx = icf.createSingleKeyNonTxInvocationContext();
            CompletionStage<?> putStage = invokePut(segmentId, ctx, e);
            aggregateStage.dependsOn(putStage.exceptionally(t -> {
               logApplyException(t, e.getKey());
               return null;
            }));
         }
         return aggregateStage.freeze();
      }
   }

   private CompletionStage<?> invoke1PCPrepare(LocalTransaction localTransaction) {
      PrepareCommand prepareCommand;
      if (Configurations.isTxVersioned(configuration)) {
         prepareCommand = commandsFactory.buildVersionedPrepareCommand(localTransaction.getGlobalTransaction(),
                                                                       localTransaction.getModifications(), true);
      } else {
         prepareCommand = commandsFactory.buildPrepareCommand(localTransaction.getGlobalTransaction(),
                                                              localTransaction.getModifications(), true);
      }
      LocalTxInvocationContext ctx = icf.createTxInvocationContext(localTransaction);
      return interceptorChain.invokeAsync(ctx, prepareCommand);
   }

   private CompletionStage<?> invokeRollback(LocalTransaction localTransaction) {
      RollbackCommand prepareCommand = commandsFactory.buildRollbackCommand(localTransaction.getGlobalTransaction());
      LocalTxInvocationContext ctx = icf.createTxInvocationContext(localTransaction);
      return interceptorChain.invokeAsync(ctx, prepareCommand);
   }

   private CompletableFuture<?> invokePut(int segmentId, InvocationContext ctx, InternalCacheEntry<?, ?> e) {
      // CallInterceptor will preserve the timestamps if the metadata is an InternalMetadataImpl instance
      InternalMetadataImpl metadata = new InternalMetadataImpl(e);
      PutKeyValueCommand put = commandsFactory.buildPutKeyValueCommand(e.getKey(), e.getValue(), segmentId,
                                                                       metadata, STATE_TRANSFER_FLAGS);
      put.setInternalMetadata(e.getInternalMetadata());
      ctx.setLockOwner(put.getKeyLockOwner());
      return interceptorChain.invokeAsync(ctx, put);
   }

   private void logApplyException(Throwable t, Object key) {
      if (!cache.wired().getStatus().allowInvocations()) {
         log.tracef("Cache %s is shutting down, stopping state transfer", cacheName);
      } else {
         log.problemApplyingStateForKey(t.getMessage(), key, t);
      }
   }

   private void applyTransactions(Address sender, Collection<TransactionInfo> transactions, int topologyId) {
      log.debugf("Applying %d transactions for cache %s transferred from node %s", transactions.size(), cacheName, sender);
      if (isTransactional) {
         for (TransactionInfo transactionInfo : transactions) {
            GlobalTransaction gtx = transactionInfo.getGlobalTransaction();
            if (rpcManager.getAddress().equals(gtx.getAddress())) {
               continue; // it is a transaction originated in this node. can happen with partition handling
            }
            // Mark the global transaction as remote. Only used for logging, hashCode/equals ignore it.
            gtx.setRemote(true);

            CacheTransaction tx = transactionTable.getLocalTransaction(gtx);
            if (tx == null) {
               tx = transactionTable.getRemoteTransaction(gtx);
               if (tx == null) {
                  try {
                     tx = transactionTable.getOrCreateRemoteTransaction(gtx, transactionInfo.getModifications());
                     // Force this node to replay the given transaction data by making it think it is 1 behind
                     ((RemoteTransaction) tx).setLookedUpEntriesTopology(topologyId - 1);
                  } catch (Throwable t) {
                     if (trace)
                        log.tracef(t, "Failed to create remote transaction %s", gtx);
                  }
               }
            }
            if (tx != null) {
               transactionInfo.getLockedKeys().forEach(tx::addBackupLockForKey);
            }
         }
      }
   }

   // Must run after the PersistenceManager
   @Start(priority = 20)
   public void start() {
      cacheName = cache.wired().getName();
      isInvalidationMode = configuration.clustering().cacheMode().isInvalidation();
      isTransactional = configuration.transaction().transactionMode().isTransactional();
      timeout = configuration.clustering().stateTransfer().timeout();

      CacheMode mode = configuration.clustering().cacheMode();
      isFetchEnabled = mode.needsStateTransfer() &&
              (configuration.clustering().stateTransfer().fetchInMemoryState() || configuration.persistence().fetchPersistentState());

      rpcOptions = new RpcOptions(DeliverOrder.NONE, timeout, TimeUnit.MILLISECONDS);

      running = true;
   }

   @Stop(priority = 0)
   @Override
   public void stop() {
      if (trace) {
         log.tracef("Shutting down StateConsumer of cache %s on node %s", cacheName, rpcManager.getAddress());
      }
      running = false;

      try {
         // cancel the inbound transfer
         synchronized (transfersLock) {
            if (inboundTransfer != null) {
               inboundTransfer.cancel();
            }
         }
      } catch (Throwable t) {
         log.errorf(t, "Failed to stop StateConsumer of cache %s on node %s", cacheName, rpcManager.getAddress());
      }
   }

   public void setKeyInvalidationListener(KeyInvalidationListener keyInvalidationListener) {
      this.keyInvalidationListener = keyInvalidationListener;
   }

   // not used in scattered cache
   private void requestSegments(IntSet addedSegments) {
      synchronized (transfersLock) {
         pendingSegments.addAll(addedSegments);
      }

      startNextTransfer();
   }

   private void startNextTransfer() {
      InboundTransferTask addedTransfer;
      synchronized (transfersLock) {
         if (this.inboundTransfer == null) {
            addedTransfer = findNextInboundTransfer();
            this.inboundTransfer = addedTransfer;
         } else {
            addedTransfer = null;
         }
      }

      if (addedTransfer != null) {
         addedTransfer.requestSegments().whenComplete((ignored, throwable) -> onTaskCompletion(addedTransfer));
         if (trace) log.tracef("Finished adding inbound state transfer %s", addedTransfer);
      }
   }

   @GuardedBy("transfersLock")
   private InboundTransferTask findNextInboundTransfer() {
      if (pendingSegments.isEmpty())
         return null;

      int[] pendingSegmentsArray = pendingSegments.toIntArray();
      int index = ThreadLocalRandom.current().nextInt(pendingSegmentsArray.length);
      int segment = pendingSegmentsArray[index];

      ConsistentHash newReadCH = cacheTopology.getReadConsistentHash();
      Address source = newReadCH.locatePrimaryOwnerForSegment(segment);
      IntSet segments = IntSets.mutableFrom(newReadCH.getPrimarySegmentsForOwner(source));
      segments.retainAll(pendingSegments);
      return new InboundTransferTask(segments, source, cacheTopology.getTopologyId(),
                                     rpcManager, commandsFactory, timeout, cacheName, true);
   }

   private Map<Address, IntSet> findSources(IntSet segments, ConsistentHash readCH) {
      if (cache.wired().getStatus().isTerminated())
         return Collections.emptyMap();

      Map<Address, IntSet> sources = new HashMap<>();
      for (Address source : readCH.getMembers()) {
         IntSet sourceSegments = IntSets.mutableFrom(readCH.getPrimarySegmentsForOwner(source));
         sourceSegments.retainAll(segments);
         if (!sourceSegments.isEmpty()) {
            sources.put(source, sourceSegments);
         }
      }
      return sources;
   }

   private CompletionStage<Void> requestTransactions(IntSet segments) {
      Map<Address, IntSet> sources = findSources(segments, cacheTopology.getReadConsistentHash());

      AggregateCompletionStage<Void> aggregateStage = CompletionStages.aggregateCompletionStage();
      int topologyId = cacheTopology.getTopologyId();
      for (Map.Entry<Address, IntSet> sourceEntry : sources.entrySet()) {
         Address source = sourceEntry.getKey();
         IntSet segmentsFromSource = sourceEntry.getValue();
         CompletionStage<Response> sourceStage = getTransactions(source, segmentsFromSource, topologyId)
               .whenComplete((response, throwable) -> processTransactionsResponse(topologyId,
                                           source, segmentsFromSource, response, throwable));
         aggregateStage.dependsOn(sourceStage);
      }

      return aggregateStage.freeze();
   }

   private void processTransactionsResponse(int topologyId, Address source, IntSet segmentsFromSource,
                                            Response response, Throwable throwable) {
      if (throwable == null) {
         if (response instanceof SuccessfulResponse) {
            List<TransactionInfo> transactions =
                  (List<TransactionInfo>) ((SuccessfulResponse) response).getResponseValue();
            applyTransactions(source, transactions, topologyId);
         } else if (response instanceof CacheNotFoundResponse) {
            log.debugf("Source %s doesn't have cache %s in topology %d, will retry in next topology",
                       source, cacheName, topologyId);
         } else {
            log.unsuccessfulResponseRetrievingTransactionsForSegments(source, response);
         }
      } else {
         if (cache.wired().getStatus().isTerminated()) {
            log.tracef("Cache %s has stopped while requesting transactions", cacheName);
         } else {
            log.failedToRetrieveTransactionsForSegments(cacheName, source, segmentsFromSource, throwable);
         }
      }
   }

   private CompletionStage<Collection<ClusterListenerReplicateCallable<Object, Object>>> getClusterListeners(
         int topologyId, List<Address> sources) {
      // Try the first member. If the request fails, fall back to the second member and so on.
      if (sources.isEmpty()) {
         if (trace)
            log.trace("Unable to acquire cluster listeners from other members, assuming none are present");
         return CompletableFuture.completedFuture(Collections.emptySet());
      }

      Address source = sources.get(0);
      // Don't send the request to self
      if (sources.get(0).equals(rpcManager.getAddress())) {
         return getClusterListeners(topologyId, sources.subList(1, sources.size()));
      }
      if (trace)
         log.tracef("Requesting cluster listeners of cache %s from node %s", cacheName, sources);

      CacheRpcCommand cmd = commandsFactory.buildStateTransferGetListenersCommand(topologyId);

      CompletionStage<ValidResponse> remoteStage =
            rpcManager.invokeCommand(source, cmd, SingleResponseCollector.validOnly(), rpcOptions);
      return handleAndCompose(remoteStage, (response, throwable) -> {
         if (throwable != null) {
            log.exceptionDuringClusterListenerRetrieval(source, throwable);
         }

         if (response instanceof SuccessfulResponse) {
            return CompletableFuture.completedFuture(
                  (Collection<ClusterListenerReplicateCallable<Object, Object>>) response.getResponseValue());
         } else {
            log.unsuccessfulResponseForClusterListeners(source, response);
            return getClusterListeners(topologyId, sources.subList(1, sources.size()));
         }
      });
   }

   private CompletionStage<Response> getTransactions(Address source, IntSet segments, int topologyId) {
      if (trace) {
         log.tracef("Requesting transactions from node %s for segments %s", source, segments);
      }
      // get transactions and locks
      CacheRpcCommand cmd = commandsFactory.buildStateTransferGetTransactionsCommand(topologyId, segments);
      return rpcManager.invokeCommand(source, cmd, PassthroughSingleResponseCollector.INSTANCE, rpcOptions);
   }

   /**
    * Cancel transfers for segments we no longer own.
    *
    * @param removedSegments segments to be cancelled
    */
   protected void cancelTransfers(IntSet removedSegments) {
      InboundTransferTask inboundTransfer = null;
      synchronized (transfersLock) {
         if (this.inboundTransfer == null)
            return;

         inboundTransfer = this.inboundTransfer;
      }
      inboundTransfer.cancelSegments(removedSegments);
   }

   protected CompletionStage<Void> removeStaleData(final IntSet removedSegments) {
      // Invalidation doesn't ever remove stale data
      if (configuration.clustering().cacheMode().isInvalidation()) {
         return CompletableFutures.completedNull();
      }
      log.debugf("Removing no longer owned entries for cache %s", cacheName);
      if (keyInvalidationListener != null) {
         keyInvalidationListener.beforeInvalidation(removedSegments, IntSets.immutableEmptySet());
      }

      // This has to be invoked before removing the segments on the data container
      localPublisherManager.segmentsLost(removedSegments);

      dataContainer.removeSegments(removedSegments);

      // We have to invoke removeSegments above on the data container. This is always done in case if L1 is enabled. L1
      // store removes all the temporary entries when removeSegments is invoked. However there is no reason to mess
      // with the store if no segments are removed, so just exit early.
      if (removedSegments.isEmpty())
         return CompletableFutures.completedNull();

      return persistenceManager.removeSegments(removedSegments)
                               .thenCompose(removed -> invalidateStaleEntries(removedSegments, removed));
   }

   private CompletionStage<Void> invalidateStaleEntries(IntSet removedSegments, Boolean removed) {
      // If there are no stores that couldn't remove segments, we don't have to worry about invaliding entries
      if (removed) {
         return CompletableFutures.completedNull();
      }

      // All these segments have been removed from the data container, so we only care about private stores
      AtomicLong removedEntriesCounter = new AtomicLong();
      Predicate<Object> filter = key -> removedSegments.contains(getSegment(key));
      Publisher<Object> publisher = persistenceManager.publishKeys(filter, PRIVATE);
      return Flowable.fromPublisher(publisher)
                     .onErrorResumeNext(throwable -> {
                        PERSISTENCE.failedLoadingKeysFromCacheStore(throwable);
                        return Flowable.empty();
                     })
                     .buffer(configuration.clustering().stateTransfer().chunkSize())
                     .concatMapCompletable(keysToRemove -> {
                        removedEntriesCounter.addAndGet(keysToRemove.size());
                        return Completable.fromCompletionStage(invalidateBatch(keysToRemove));
                     })
                     .toCompletionStage(null)
                     .thenRun(() -> {
                        if (trace) log.tracef("Removed %d keys, data container now has %d keys",
                                              removedEntriesCounter.get(), dataContainer.sizeIncludingExpired());
                     });
   }

   protected CompletionStage<Void> invalidateBatch(Collection<Object> keysToRemove) {
      InvalidateCommand invalidateCmd = commandsFactory.buildInvalidateCommand(
            EnumUtil.bitSetOf(CACHE_MODE_LOCAL, SKIP_LOCKING), keysToRemove.toArray());
      InvocationContext ctx = icf.createNonTxInvocationContext();
      ctx.setLockOwner(invalidateCmd.getKeyLockOwner());
      return interceptorChain.invokeAsync(ctx, invalidateCmd)
                             .handle((ignored, throwable) -> {
                                if (throwable instanceof IllegalLifecycleStateException) {
                                   // Ignore shutdown-related errors, because InvocationContextInterceptor starts
                                   // rejecting commands before any component is stopped
                                } else if (throwable != null) {
                                   log.failedToInvalidateKeys(throwable);
                                }
                                return null;
                             });
   }

   /**
    * Check if the active transfer should be restarted from a different source because the initial source
    * is no longer a member.
    *
    * If a transfer has already failed, e.g. because the source crashed, give it another chance
    * in the new topology.
    */
   private void restartBrokenTransfers(CacheTopology cacheTopology) {
      InboundTransferTask transferToCancel = null;
      synchronized (transfersLock) {
         // First reset the failed segments
         pendingSegments.addAll(failedSegments);
         failedSegments.clear();

         // Then cancel the active transfer if its source is no longer a member
         if (inboundTransfer == null)
            return;

         Address source = inboundTransfer.getSource();
         List<Address> members = cacheTopology.getReadConsistentHash().getMembers();
         if (!members.contains(source)) {
            if (trace) {
               log.tracef("Removing inbound transfers from non-member %s: %s", source, inboundTransfer);
            }

            transferToCancel = this.inboundTransfer;
            this.inboundTransfer = null;
         }
      }
      if (transferToCancel != null) {
         transferToCancel.cancel();
      }
   }

   private int getSegment(Object key) {
      // here we can use any CH version because the routing table is not involved in computing the segment
      return keyPartitioner.getSegment(key);
   }

   protected void removeTransfer() {
      synchronized (transfersLock) {
         if (inboundTransfer == null)
            return;

         if (trace) log.tracef("Removing inbound transfers from node %s: %s",
                               inboundTransfer.getSource(), inboundTransfer);
         this.inboundTransfer = null;
      }
   }

   private void finishTransferSegments() {
      synchronized (transfersLock) {
         IntSet segments = inboundTransfer.getSegments();
         if (!inboundTransfer.isCancelled()) {
            this.pendingSegments.removeAll(segments);
            if (!inboundTransfer.isCompletedSuccessfully()) {
               this.failedSegments.addAll(segments);
            } else {
               this.receivedSegments.addAll(segments);
            }
         }
      }
   }

   protected void onTaskCompletion(final InboundTransferTask inboundTransfer) {
      try {
         if (trace) log.tracef("Inbound transfer finished: %s", inboundTransfer);
         finishTransferSegments();
         removeTransfer();
         startNextTransfer();
         if (inboundTransfer.isCompletedSuccessfully()) {
            notifyEndOfStateTransferIfNeeded();
         }
      } catch (Throwable t) {
         CLUSTER.stateTransferError(cacheName, t);
      }
   }

   public interface KeyInvalidationListener {
      void beforeInvalidation(IntSet removedSegments, IntSet staleL1Segments);
   }

   private static class ApplyStateTransaction extends TransactionImpl {
      // Make it different from embedded txs (1)
      static final int FORMAT_ID = 2;
      AtomicLong id = new AtomicLong(0);

      ApplyStateTransaction() {
         byte[] bytes = new byte[8];
         Util.longToBytes(id.incrementAndGet(), bytes, 0);
         XidImpl xid = XidImpl.create(FORMAT_ID, bytes, bytes);
         setXid(xid);
      }
   }
}
