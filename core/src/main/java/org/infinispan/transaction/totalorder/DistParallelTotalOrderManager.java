package org.infinispan.transaction.totalorder;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.remote.PrepareResponseCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

/**
 * The total order manager implementation the handles the distributed mode, namely the send of the write skew outcome
 * to the transaction originator
 *
 * This implementation uses a thread pool to validate transaction
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
@MBean(objectName = "TotalOrderManager", description = "Concurrent total order management for distributed mode")
public class DistParallelTotalOrderManager extends ParallelTotalOrderManager {

   private static final Log log = LogFactory.getLog(DistParallelTotalOrderManager.class);

   private CommandsFactory commandsFactory;
   private DistributionManager distributionManager;
   private RpcManager rpcManager;

   @Inject
   public void inject(CommandsFactory commandsFactory, DistributionManager distributionManager, RpcManager rpcManager) {
      this.commandsFactory = commandsFactory;
      this.distributionManager = distributionManager;
      this.rpcManager = rpcManager;
   }

   @Override
   public void addLocalTransaction(GlobalTransaction globalTransaction, LocalTransaction localTransaction) {
      super.addLocalTransaction(globalTransaction, localTransaction);
      localTransaction.initToCollectAcks(Util.getAffectedKeys(localTransaction.getModifications(), dataContainer));
   }

   @Override
   protected ParallelPrepareProcessor constructParallelPrepareProcessor(PrepareCommand prepareCommand, TxInvocationContext ctx,
                                                                        CommandInterceptor invoker,
                                                                        TotalOrderRemoteTransaction totalOrderRemoteTransaction) {
      return new DistributedParallelPrepareProcessor(prepareCommand, ctx, invoker, totalOrderRemoteTransaction);
   }

   private class DistributedParallelPrepareProcessor extends ParallelPrepareProcessor {

      private DistributedParallelPrepareProcessor(PrepareCommand prepareCommand, TxInvocationContext txInvocationContext,
                                                  CommandInterceptor invoker, TotalOrderRemoteTransaction totalOrderRemoteTransaction) {
         super(prepareCommand, txInvocationContext, invoker, totalOrderRemoteTransaction);
      }

      @Override
      protected void finalizeProcessing(Object result, boolean exception) {
         remoteTransaction.markPreparedAndNotify();
         GlobalTransaction gtx = prepareCommand.getGlobalTransaction();
         LocalTransaction localTransaction = getLocalTransaction(gtx);

         if (prepareCommand.isOnePhaseCommit() || remoteTransaction.isMarkedForRollback()) {
            markTxCompleted();
            removeLocalTransaction(gtx);
            return;
         } else if (exception) {
            if (localTransaction != null) {
               //the tx is local. release the keys and remove the remote transaction
               markTxCompleted();
               //we can remove the local transaction. The others responses are not needed
               removeLocalTransaction(gtx);

            } else {
               //the tx is remote. release the resources but don't remove the remote transaction because
               // the rollback will be received later
               finishTransaction(remoteTransaction);
            }
         }

         if (localTransaction != null) {
            //transaction is local, so add directly
            if (exception) {
               localTransaction.addException((Exception) result, true);
            } else {
               localTransaction.addKeysValidated(getModifiedKeyFromModifications(remoteTransaction.getModifications()),
                                                 true);
            }
         } else {
            //send the response            
            PrepareResponseCommand response = commandsFactory.buildPrepareResponseCommand(gtx);
            if (exception) {
               response.addResult(result);
            } else {
               response.addResult(getModifiedKeyFromModifications(remoteTransaction.getModifications()));
            }

            if (trace) {
               log.tracef("Send the Prepare Response Command %s back to originator", response);
            }

            try {
               rpcManager.invokeRemotely(Collections.singleton(gtx.getAddress()), response, false);
            } catch (Throwable throwable) {
               log.exceptionWhileSendingPrepareResponseCommand(throwable);
            }
         }
      }
   }

   @Override
   protected Set<Object> getModifiedKeyFromModifications(Collection<WriteCommand> modifications) {
      Set<Object> localKeys = super.getModifiedKeyFromModifications(modifications);

      for (Iterator<Object> iterator = localKeys.iterator(); iterator.hasNext(); ) {
         if (!distributionManager.getLocality(iterator.next()).isLocal()) {
            iterator.remove();
         }
      }

      return localKeys;
   }
}
