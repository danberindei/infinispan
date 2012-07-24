package org.infinispan.transaction.totalorder;

import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.remoting.RpcException;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.TxDependencyLatch;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.rhq.helpers.pluginAnnotations.agent.DisplayType;
import org.rhq.helpers.pluginAnnotations.agent.Metric;
import org.rhq.helpers.pluginAnnotations.agent.Units;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author mircea.markus@jboss.com
 * @author Pedro Ruivo
 * @since 5.2.0
 */
public abstract class BaseTotalOrderManager implements TotalOrderManager {

   private static final Log log = LogFactory.getLog(BaseTotalOrderManager.class);
   protected static boolean trace;

   protected Configuration configuration;
   protected InvocationContextContainer invocationContextContainer;

   protected TransactionTable transactionTable;
   protected DataContainer dataContainer;

   protected final AtomicLong processingDuration = new AtomicLong(0);
   protected final AtomicInteger numberOfTxValidated = new AtomicInteger(0);

   /**
    * Map between GlobalTransaction and LocalTransaction. Used to sync the threads in remote validation and the
    * transaction execution thread.
    */
   private final ConcurrentMap<GlobalTransaction, LocalTransaction> localTransactionMap =
         new ConcurrentHashMap<GlobalTransaction, LocalTransaction>();


   /**
    * Volatile as its value can be changed by a JMX thread.
    */
   protected volatile boolean statisticsEnabled;
   private boolean isSync;

   @Inject
   public void inject(Configuration configuration, InvocationContextContainer invocationContextContainer,
                      TransactionTable transactionTable, DataContainer dataContainer) {
      this.configuration = configuration;
      this.invocationContextContainer = invocationContextContainer;
      this.transactionTable = transactionTable;
      this.dataContainer = dataContainer;
   }

   @Start
   public void start() {
      trace = log.isTraceEnabled();
      setStatisticsEnabled(configuration.jmxStatistics().enabled());
      isSync = configuration.clustering().cacheMode().isSynchronous();
   }

   @Override
   public void addLocalTransaction(GlobalTransaction globalTransaction, LocalTransaction localTransaction) {
      localTransactionMap.put(globalTransaction, localTransaction);
   }

   @Override
   public final boolean waitForPrepareToSucceed(TxInvocationContext ctx) {
      if (!ctx.isOriginLocal()) throw new IllegalStateException();
      boolean shouldBeRetransmitted = false;

      if (isSync) {

         //in sync mode, blocks in the LocalTransaction
         if (trace)
            log.tracef("Transaction [%s] sent in synchronous mode. waiting until prepare is processed locally.",
                       ctx.getGlobalTransaction().prettyPrint());

         LocalTransaction localTransaction = (LocalTransaction) ctx.getCacheTransaction();
         try {
            localTransaction.awaitUntilModificationsApplied();
            shouldBeRetransmitted = localTransaction.isMarkedToRetransmit();
            if (trace)
               log.tracef("Prepare succeeded on time for transaction %s, waking up..", ctx.getGlobalTransaction().prettyPrint());
         } catch (Throwable th) {
            if (trace)
               log.tracef(th, "Transaction %s hasn't prepare correctly", ctx.getGlobalTransaction().prettyPrint());
            throw new RpcException(th);
         } finally {
            //the transaction is no longer needed
            if (!shouldBeRetransmitted) {
               localTransactionMap.remove(ctx.getGlobalTransaction());
            }
         }
      }
      return shouldBeRetransmitted;
   }

   @Override
   public void notifyStateTransferInProgress(GlobalTransaction globalTransaction, Throwable e) {
      LocalTransaction localTransaction = localTransactionMap.get(globalTransaction);
      if (localTransaction != null) {
         localTransaction.addPrepareResult(e, true);
      }
   }

   @Override
   public final void finishTransaction(GlobalTransaction gtx, boolean ignoreNullTxInfo, TotalOrderRemoteTransaction transaction) {
      if (trace) log.tracef("transaction %s is finished", gtx.prettyPrint());

      TotalOrderRemoteTransaction remoteTransaction = (TotalOrderRemoteTransaction) transactionTable.removeRemoteTransaction(gtx);

      if (remoteTransaction == null) {
         remoteTransaction = transaction;
      }

      if (remoteTransaction != null) {
         finishTransaction(remoteTransaction);
      } else if (!ignoreNullTxInfo) {
         log.remoteTransactionIsNull(gtx.prettyPrint());
      }
   }

   @Override
   public final boolean waitForTxPrepared(TotalOrderRemoteTransaction remoteTransaction, boolean commit,
                                          EntryVersionsMap newVersions) {
      GlobalTransaction gtx = remoteTransaction.getGlobalTransaction();
      if (trace)
         log.tracef("%s command received. Waiting until transaction %s is prepared. New versions are %s",
                    commit ? "Commit" : "Rollback", gtx.prettyPrint(), newVersions);

      boolean needsToProcessCommand;
      try {
         needsToProcessCommand = remoteTransaction.waitPrepared(commit, newVersions);
         if (trace) log.tracef("Transaction %s successfully finishes the waiting time until prepared. " +
                                     "%s command will be processed? %s", gtx.prettyPrint(),
                               commit ? "Commit" : "Rollback", needsToProcessCommand ? "yes" : "no");
      } catch (InterruptedException e) {
         log.timeoutWaitingUntilTransactionPrepared(gtx.prettyPrint());
         needsToProcessCommand = false;
      }
      return needsToProcessCommand;
   }

   /**
    * Remove the keys from the map (if their didn't change) and release the count down latch, unblocking the next
    * transaction
    * @param remoteTransaction the remote transaction
    */
   protected void finishTransaction(TotalOrderRemoteTransaction remoteTransaction) {
      TxDependencyLatch latch = remoteTransaction.getLatch();
      if (trace) log.tracef("Releasing resources for transaction %s", remoteTransaction);
      latch.countDown();
   }


   @ManagedAttribute(description = "Average duration of a transaction validation (milliseconds)")
   @Metric(displayName = "Average Validation Duration", units = Units.MILLISECONDS, displayType = DisplayType.SUMMARY)
   public double getAverageValidationDuration() {
      long time = processingDuration.get();
      int tx = numberOfTxValidated.get();
      if (tx == 0) {
         return 0;
      }
      return (time / tx) / 1000000.0;
   }

   @ManagedOperation(description = "Resets the statistics")
   public void resetStatistics() {
      processingDuration.set(0);
      numberOfTxValidated.set(0);
   }

   @ManagedAttribute(description = "Show it the gathering of statistics is enabled")
   public boolean isStatisticsEnabled() {
      return statisticsEnabled;
   }

   @ManagedOperation(description = "Enables or disables the gathering of statistics by this component")
   public void setStatisticsEnabled(boolean statisticsEnabled) {
      this.statisticsEnabled = statisticsEnabled;
   }

   protected final void updateLocalTransaction(Object result, boolean exception, GlobalTransaction gtx) {
      LocalTransaction localTransaction = localTransactionMap.get(gtx);

      if (localTransaction != null) {
         localTransaction.addPrepareResult(result, exception);
         localTransactionMap.remove(gtx);
      } else {
         log.tracef("There's no local transaction corresponding to this(%s) remote transaction", gtx);
      }
   }

   protected final long now() {
      //we know that this is only used for stats
      return statisticsEnabled ? System.nanoTime() : -1;
   }

   protected final void copyLookedUpEntriesToRemoteContext(TxInvocationContext ctx) {
      LocalTransaction localTransaction = localTransactionMap.get(ctx.getGlobalTransaction());
      if (localTransaction != null) {
         ctx.putLookedUpEntries(localTransaction.getLookedUpEntries());
      }
   }

   protected final void logAndCheckContext(PrepareCommand prepareCommand, TxInvocationContext ctx) {
      if (trace) log.tracef("Processing transaction from sequencer: %s", prepareCommand.getGlobalTransaction().prettyPrint());

      if (ctx.isOriginLocal()) throw new IllegalArgumentException("Local invocation not allowed!");
   }

   protected final void removeLocalTransaction(GlobalTransaction globalTransaction) {
      localTransactionMap.remove(globalTransaction);
   }

   public final LocalTransaction getLocalTransaction(GlobalTransaction globalTransaction) {
      return localTransactionMap.get(globalTransaction);
   }

   /**
    * calculates the keys affected by the list of modification. This method should return only the key own by this node
    * @param modifications the list of modifications
    * @return a set of local keys
    */
   protected Set<Object> getModifiedKeyFromModifications(Collection<WriteCommand> modifications) {
      if (modifications == null) {
         return Collections.emptySet();
      }
      return Util.getAffectedKeys(modifications, dataContainer);
   }

   @Override
   public Set<TxDependencyLatch> getPendingCommittingTransaction() {
      return Collections.emptySet();
   }
}
