package org.infinispan.statetransfer.totalorder;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.statetransfer.StateTransferLockImpl;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.TimeoutException;

/**
 * The state transfer lock for Total Order based protocols
 *
 * in the description of the state transfer protocol, the prepares delivered during the state transfer progress
 * should be block until the state transfer ends. In this case we can have 2 options:
 *  - the view is rolled back: the prepare can be processed normally
 *  - the view is committed: the prepare should be retransmitted
 *
 * Note: the commit and rollbacks can pass because they don't change the data
 *       it needs prepare + commit/rollback and the prepares are blocked :)
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class TotalOrderStateTransferLockImpl extends StateTransferLockImpl {
   private static final Log log = LogFactory.getLog(TotalOrderStateTransferLockImpl.class);

   @Override
   public boolean acquireForCommand(TxInvocationContext ctx, PrepareCommand command) throws InterruptedException, TimeoutException {
      //NOTE the semantic of acquireForCommand is a little different now!!
      waitForStateTransferToEnd();
      return true;
   }

   @Override
   public void releaseForCommand(TxInvocationContext ctx, PrepareCommand command) {
      //no-op
   }

   /**
    * it blocks prepares if the state transfer is in progress and the prepare has the same cache view than the current
    * cache view (see class description). The prepare is unblocked when the state transfer is finished
    *
    * this method does nothing if no state transfer is in progress.
    *
    * @throws InterruptedException
    */
   private void waitForStateTransferToEnd() throws InterruptedException {
      synchronized (lock) {
         while (areNewTransactionsBlocked()) {
            log.tracef("blocking thread until the until the state transfer has finished");
            lock.wait();
         }
      }
      log.tracef("No state transfer or state transfer has finished. move on...");
   }

   @Override
   public void waitForStateTransferToEnd(InvocationContext ctx, VisitableCommand command, int newCacheViewId) throws TimeoutException, InterruptedException {
      if (command instanceof PrepareCommand || command instanceof CommitCommand || command instanceof RollbackCommand) {
         return; //the prepare commands does not acquire the lock
      }
      super.waitForStateTransferToEnd(ctx, command, newCacheViewId);
   }
}
