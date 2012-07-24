package org.infinispan.interceptors.totalorder;

import org.infinispan.cacheviews.CacheViewsManager;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.StateTransferLockInterceptor;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.totalorder.TotalOrderManager;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @author Mircea Markus <mircea.markus@jboss.com> (C) 2011 Red Hat Inc.
 * @author Pedro Ruivo
 * @since 5.2
 */
public class TotalOrderStateTransferLockInterceptor extends StateTransferLockInterceptor {

   private static final Log log = LogFactory.getLog(TotalOrderStateTransferLockInterceptor.class);

   private CacheViewsManager cacheViewsManager;
   private TotalOrderManager totalOrderManager;

   @Inject
   public void init(CacheViewsManager cacheViewsManager, TotalOrderManager totalOrderManager) {
      this.cacheViewsManager = cacheViewsManager;
      this.totalOrderManager = totalOrderManager;
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      stateTransferLock.acquireForCommand(ctx, command);
      if (ctx.isOriginLocal()) {
         setTransactionViewId(command.getGlobalTransaction(), command.getCacheName());
      }

      if (shouldRetransmit(command)) {
         LocalTransaction ltx = totalOrderManager.getLocalTransaction(command.getGlobalTransaction());
         log.tracef("Transaction %s should be retransmitted. Local transaction is %s", command.getGlobalTransaction().prettyPrint(),
                    ltx);
         if (ltx != null) {
            setTransactionViewId(ltx.getGlobalTransaction(), command.getCacheName());
            ltx.markToRetransmit();
         }
         return null;
      }
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      return invokeNextInterceptor(ctx, command);
   }

   private boolean shouldRetransmit(PrepareCommand command) {
      //TODO be smarter: if is full replication and it is a leave, return false
      return cacheViewsManager.getCommittedView(command.getCacheName()).getViewId() > command.getGlobalTransaction().getViewId();
   }

   private void setTransactionViewId(GlobalTransaction globalTransaction, String cacheName) {
      globalTransaction.setViewId(cacheViewsManager.getCommittedView(cacheName).getViewId());
      log.tracef("Set view id [%s] for transaction %s", globalTransaction.getViewId(), globalTransaction.prettyPrint());
   }
}
