package org.infinispan.interceptors.totalorder;

import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.VersionedDistributionInterceptor;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.totalorder.TotalOrderManager;
import org.infinispan.util.Util;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Arrays;
import java.util.Collection;

import static org.infinispan.transaction.WriteSkewHelper.setVersionsSeenOnPrepareCommand;

/**
 * This interceptor is used in total order in distributed mode when the write skew check is enabled.
 * After sending the prepare through TOA (Total Order Anycast), it blocks the execution thread until the transaction 
 * outcome is know (i.e., the write skew check passes in all keys owners)
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class TotalOrderVersionedDistributionInterceptor extends VersionedDistributionInterceptor {

   private static final Log log = LogFactory.getLog(TotalOrderVersionedDistributionInterceptor.class);

   private TotalOrderManager totalOrderManager;

   @Inject
   public void injectDependencies(TotalOrderManager totalOrderManager) {
      this.totalOrderManager = totalOrderManager;
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      //this map is only populated after locks are acquired. However, no locks are acquired when total order is enabled
      //so we need to populate it here
      ctx.addAllAffectedKeys(Util.getAffectedKeys(Arrays.asList(command.getModifications()), dataContainer));

      Object result;
      boolean shouldRetransmit;

      do{
         result = super.visitPrepareCommand(ctx, command);
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

   @Override
   protected void prepareOnAffectedNodes(TxInvocationContext ctx, PrepareCommand command,
                                         Collection<Address> recipients, boolean sync) {
      if(log.isTraceEnabled()) {
         log.tracef("Total Order Anycast transaction %s with Total Order", command.getGlobalTransaction().prettyPrint());
      }

      if (!(command instanceof VersionedPrepareCommand)) {
         throw new IllegalStateException("Expected a Versioned Prepare Command in version aware component");
      }

      //In total order based protocol, we use synchronous prepare only when the commit phase is synchronous.
      //It is needed for this case to ensure the semantic of sync commit phase and to ensure that all nodes
      //   has seen the transaction (remember: the commit can be deliver before without this)
      boolean reallySync = configuration.isSyncCommitPhase();
      setVersionsSeenOnPrepareCommand((VersionedPrepareCommand) command, ctx);
      try {
         rpcManager.invokeRemotely(recipients, command, reallySync, false, true);
      } catch (TimeoutException e) {
         //just ignore. it will be deliver for sure
      }
   }
}
