package org.infinispan.statetransfer.totalorder;

import org.infinispan.factories.annotations.Inject;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.DistributedStateTransferManagerImpl;
import org.infinispan.statetransfer.DistributedStateTransferTask;
import org.infinispan.transaction.totalorder.TotalOrderManager;

import java.util.List;

/**
 * The replicated mode implementation of {@link org.infinispan.statetransfer.StateTransferManager} that it is aware that
 * total order protocol is in use
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class TotalOrderDistributedStateTransferManagerImpl extends DistributedStateTransferManagerImpl {

   private TotalOrderManager totalOrderManager;

   @Inject
   public void inject(TotalOrderManager totalOrderManager) {
      this.totalOrderManager = totalOrderManager;
   }

   @Override
   protected DistributedStateTransferTask createStateTransferTask(int viewId, List<Address> members, boolean initialView) {
      return new TotalOrderDistributedStateTransferTask(rpcManager, configuration, dataContainer,
                                                        this, dm, stateTransferLock, cacheNotifier, viewId, members, chOld, chNew,
                                                        initialView, transactionTable, totalOrderManager);
   }

   @Override
   protected boolean usePriorityQueue() {
      return true;
   }
}
