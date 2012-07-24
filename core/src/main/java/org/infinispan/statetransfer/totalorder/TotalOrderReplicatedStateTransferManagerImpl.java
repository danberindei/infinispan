package org.infinispan.statetransfer.totalorder;

import org.infinispan.factories.annotations.Inject;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.ReplicatedStateTransferManagerImpl;
import org.infinispan.statetransfer.ReplicatedStateTransferTask;
import org.infinispan.transaction.totalorder.TotalOrderManager;

import java.util.List;

/**
 * The replicated mode implementation of {@link org.infinispan.statetransfer.StateTransferManager} that it is aware that
 * total order protocol is in use
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class TotalOrderReplicatedStateTransferManagerImpl extends ReplicatedStateTransferManagerImpl {

   private TotalOrderManager totalOrderManager;

   @Inject
   public void inject(TotalOrderManager totalOrderManager) {
      this.totalOrderManager = totalOrderManager;
   }

   @Override
   protected ReplicatedStateTransferTask createStateTransferTask(int viewId, List<Address> members, boolean initialView) {
      return new TotalOrderReplicatedStateTransferTask(rpcManager, configuration, dataContainer, this, stateTransferLock,
                                                       cacheNotifier, viewId, members, chOld, chNew, initialView,
                                                       totalOrderManager);
   }

   @Override
   protected boolean usePriorityQueue() {
      return true;
   }
}
