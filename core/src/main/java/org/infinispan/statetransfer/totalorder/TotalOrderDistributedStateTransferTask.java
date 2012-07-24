package org.infinispan.statetransfer.totalorder;

import org.infinispan.config.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.DistributedStateTransferManagerImpl;
import org.infinispan.statetransfer.DistributedStateTransferTask;
import org.infinispan.statetransfer.StateTransferLock;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.TxDependencyLatch;
import org.infinispan.transaction.totalorder.TotalOrderManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;
import java.util.Set;

/**
 * Task the pushes the state for the new node and it is aware that total order broadcast based protocol is in use
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class TotalOrderDistributedStateTransferTask extends DistributedStateTransferTask {

   private static final Log log = LogFactory.getLog(TotalOrderDistributedStateTransferTask.class);

   private final TotalOrderManager totalOrderManager;

   public TotalOrderDistributedStateTransferTask(RpcManager rpcManager, Configuration configuration,
                                                 DataContainer dataContainer, DistributedStateTransferManagerImpl stateTransferManager,
                                                 DistributionManager dm, StateTransferLock stateTransferLock,
                                                 CacheNotifier cacheNotifier, int newViewId, Collection<Address> members,
                                                 ConsistentHash chOld, ConsistentHash chNew, boolean initialView,
                                                 TransactionTable transactionTable, TotalOrderManager totalOrderManager) {
      super(rpcManager, configuration, dataContainer, stateTransferManager, dm, stateTransferLock, cacheNotifier,
            newViewId, members, chOld, chNew, initialView, transactionTable);
      this.totalOrderManager = totalOrderManager;
   }

   @Override
   protected void beforeStartPushing() {
      Set<TxDependencyLatch> pendingTransaction = totalOrderManager.getPendingCommittingTransaction();

      if (log.isTraceEnabled()) {
         log.tracef("Waiting for pending remote transaction before pushing the data. Transaction are %s",pendingTransaction);
      }

      for (TxDependencyLatch txDependencyLatch : pendingTransaction) {
         try {
            txDependencyLatch.await();
         } catch (InterruptedException e) {
            log.warnf("Interrupted exception caught while waiting for %s", txDependencyLatch);
         }
      }
   }

   @Override
   protected boolean shouldPushLocks() {
      return false;
   }
}
