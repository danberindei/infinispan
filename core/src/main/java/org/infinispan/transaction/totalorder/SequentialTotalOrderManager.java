package org.infinispan.transaction.totalorder;

import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @author mircea.markus@jboss.com
 * @author Pedro Ruivo
 * @since 5.2.0
 */
@MBean(objectName = "TotalOrderManager", description = "Simple total order management")
public class SequentialTotalOrderManager extends BaseTotalOrderManager {

   private static final Log log = LogFactory.getLog(SequentialTotalOrderManager.class);

   public final void processTransactionFromSequencer(PrepareCommand prepareCommand, TxInvocationContext ctx, CommandInterceptor invoker) {

      logAndCheckContext(prepareCommand, ctx);

      copyLookedUpEntriesToRemoteContext(ctx);
      
      Object result = null;
      boolean exception = false;
      long startTime = now();
      try {
         result = prepareCommand.acceptVisitor(ctx, invoker);
      } catch (Throwable t) {
         log.trace("Exception while processing the rest of the interceptor chain", t);
         result = t;
         exception = true;
         //if an exception is throw, the TxInterceptor will not remove it from the TxTable and the rollback is not 
         //sent (with TO)
         transactionTable.remoteTransactionRollback(prepareCommand.getGlobalTransaction());
      } finally {
         logProcessingFinalStatus(prepareCommand, result, exception);
         updateLocalTransaction(result, exception, prepareCommand.getGlobalTransaction());
         updateProcessingDurationStats(startTime, now());
         if (exception) {
            transactionTable.remoteTransactionRollback(prepareCommand.getGlobalTransaction());
         } else {
            transactionTable.remoteTransactionCommitted(prepareCommand.getGlobalTransaction());
         }
      }
   }

   private void updateProcessingDurationStats(long start, long end) {
      if (statisticsEnabled) {
         processingDuration.addAndGet(end - start);
         numberOfTxValidated.incrementAndGet();
      }
   }


   private void logProcessingFinalStatus(PrepareCommand prepareCommand, Object result, boolean exception) {
      if (trace)
         log.tracef("Transaction %s finished processing (%s). Validation result is %s ",
                    prepareCommand.getGlobalTransaction().prettyPrint(),
                    (exception ? "failed" : "ok"), (exception ? ((Throwable) result).getMessage() : result));
   }
}
