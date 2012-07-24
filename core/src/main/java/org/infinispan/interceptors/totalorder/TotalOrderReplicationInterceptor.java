package org.infinispan.interceptors.totalorder;

import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.ReplicationInterceptor;
import org.infinispan.transaction.totalorder.TotalOrderManager;
import org.infinispan.util.concurrent.TimeoutException;

/**
 * @author mircea.markus@jboss.com
 * @since 5.2.0
 */
public class TotalOrderReplicationInterceptor extends ReplicationInterceptor {

   private TotalOrderManager totalOrderManager;

   @Inject
   public void init(TotalOrderManager totalOrderManager) {
      this.totalOrderManager = totalOrderManager;
   }

   @Override
   public final Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      Object result;
      boolean shouldRetransmit;
      do {
         try {
            result = super.visitPrepareCommand(ctx, command);
         } catch (TimeoutException e) {
            //just ignore. it will be deliver for sure
            result = null;
         }
         shouldRetransmit = false;
         if (shouldInvokeRemoteTxCommand(ctx)) {
            //we need to do the waiting here and not in the TotalOrderInterceptor because it is possible for the replication
            //not to take place, e.g. in the case there are no changes in the context. And this is the place where we know
            // if the replication occurred.
            shouldRetransmit = totalOrderManager.waitForPrepareToSucceed(ctx);
         }
      } while (shouldRetransmit);
      return result;
   }
}
