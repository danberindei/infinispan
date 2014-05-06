package org.infinispan.transaction.impl;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Base class for both Sync and XAResource enlistment adapters.
 *
 * @author Mircea Markus
 * @since 5.1
 */
public abstract class AbstractEnlistmentAdapter {

   private static Log log = LogFactory.getLog(AbstractEnlistmentAdapter.class);

   private final CommandsFactory commandsFactory;
   private final RpcManager rpcManager;
   private final TransactionTable txTable;
   private final ClusteringDependentLogic clusteringLogic;
   private final int hashCode;
   protected final TransactionCoordinator txCoordinator;

   public AbstractEnlistmentAdapter(CacheTransaction cacheTransaction,
            CommandsFactory commandsFactory, RpcManager rpcManager,
            TransactionTable txTable, ClusteringDependentLogic clusteringLogic,
            Configuration configuration, TransactionCoordinator txCoordinator) {
      this.commandsFactory = commandsFactory;
      this.rpcManager = rpcManager;
      this.txTable = txTable;
      this.clusteringLogic = clusteringLogic;
      hashCode = preComputeHashCode(cacheTransaction);
      this.txCoordinator = txCoordinator;
   }

   public AbstractEnlistmentAdapter(CommandsFactory commandsFactory,
            RpcManager rpcManager, TransactionTable txTable,
            ClusteringDependentLogic clusteringLogic, Configuration configuration, TransactionCoordinator txCoordinator) {
      this.commandsFactory = commandsFactory;
      this.rpcManager = rpcManager;
      this.txTable = txTable;
      this.clusteringLogic = clusteringLogic;
      hashCode = 31;
      this.txCoordinator = txCoordinator;
   }
   
   /**
    * Invoked by TransactionManagers, make sure it's an efficient implementation.
    * System.identityHashCode(x) is NOT an efficient implementation.
    */
   @Override
   public final int hashCode() {
      return this.hashCode;
   }

   private static int preComputeHashCode(final CacheTransaction cacheTx) {
      return 31 + cacheTx.hashCode();
   }
}
