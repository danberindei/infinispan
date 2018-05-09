package org.infinispan.blocks;

import java.util.concurrent.CompletionStage;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.interceptors.InvocationStage;
import org.infinispan.interceptors.InvocationSuccessFunction;
import org.infinispan.interceptors.SyncInvocationStage;
import org.infinispan.interceptors.impl.SimpleAsyncInvocationStage;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.statetransfer.StateTransferLock;

public class DistExecutionEngine implements ExecutionEngine {
   DistributionManager dm;
   StateTransferLock stl;

   ClusteringBlock cluster;
   ReadBlock read;
   UpdateBlock update;

   @Override
   public Object executeGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) {
      Object key = command.getKey();

      CacheEntry entry = ctx.lookupEntry(key);
      if (entry != null) return syncValue(entry.getValue());

      LocalizedCacheTopology topology = dm.getCacheTopology();
      if (topology.isReadOwner(key)) {
         return retryOnTopologyChange(ctx, command, topology, read.fetchEntry(key, command.getFlagsBitSet()),
                                      (rCtx, rCommand, rv1) ->
                                         executeGetKeyValueCommand(rCtx, ((GetKeyValueCommand) rCommand)))
                   // TODO Just put the entry in the context and compute the return value in a different method
            .thenApply(ctx, command, (rCtx, rCommand, rv) -> ((InternalCacheEntry) rv).getValue());
      }

      if (ctx.isOriginLocal() && !command.hasAnyFlag(FlagBitSets.CACHE_MODE_LOCAL)) {
         return retryOnTopologyChange(ctx, command, topology, cluster.fetchRemote(key, command.getFlagsBitSet()),
            (rCtx, rCommand, rv) -> executeGetKeyValueCommand(rCtx, ((GetKeyValueCommand) rCommand)))
                   .thenApply(ctx, command, (rCtx, rCommand, rv) -> ((InternalCacheEntry) rv).getValue());
      }

      return null;
   }

   private InvocationStage retryOnTopologyChange(InvocationContext ctx, VisitableCommand command,
                                        LocalizedCacheTopology topology, InvocationStage stage,
                                        InvocationSuccessFunction retryFunction) {
      return makeStage(stage.andHandle(ctx, command, (rCtx, rCommand, rv, throwable) -> {
         if (throwable == null)
            return rv;
         if (throwable instanceof OutdatedTopologyException ||
             throwable instanceof SuspectException)
            return asyncValue(stl.topologyFuture(topology.getTopologyId() + 1))
                      .thenApply(rCtx, rCommand, retryFunction);
         throw throwable;
      }));
   }

   public static InvocationStage syncValue(Object value) {
      return new SyncInvocationStage(value);
   }

   public static InvocationStage asyncValue(CompletionStage<?> valueFuture) {
      return new SimpleAsyncInvocationStage(valueFuture);
   }

   public static InvocationStage makeStage(Object rv) {
      if (rv instanceof InvocationStage) {
         return (InvocationStage) rv;
      } else {
         return new SyncInvocationStage(rv);
      }
   }
}