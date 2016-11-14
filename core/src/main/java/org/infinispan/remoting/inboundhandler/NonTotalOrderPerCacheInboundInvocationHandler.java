package org.infinispan.remoting.inboundhandler;

import java.util.Collection;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.remoting.inboundhandler.action.ActionState;
import org.infinispan.remoting.inboundhandler.action.DefaultReadyAction;
import org.infinispan.remoting.inboundhandler.action.LockAction;
import org.infinispan.remoting.inboundhandler.action.ReadyAction;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.util.concurrent.BlockingRunnable;
import org.infinispan.util.concurrent.locks.LockListener;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.concurrent.locks.LockState;
import org.infinispan.util.concurrent.locks.RemoteLockCommand;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * A {@link org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler} implementation for non-total order
 * caches.
 *
 * @author Pedro Ruivo
 * @since 7.1
 */
public class NonTotalOrderPerCacheInboundInvocationHandler extends BasePerCacheInboundInvocationHandler implements
      LockListener {

   private static final Log log = LogFactory.getLog(NonTotalOrderPerCacheInboundInvocationHandler.class);
   private static final boolean trace = log.isTraceEnabled();

   @Inject private LockManager lockManager;
   @Inject private ClusteringDependentLogic clusteringDependentLogic;
   private long lockTimeout;
   private boolean isLocking;

   public NonTotalOrderPerCacheInboundInvocationHandler() {
   }

   @Start
   public void start() {
      lockTimeout = configuration.locking().lockAcquisitionTimeout();
      isLocking = !configuration.clustering().cacheMode().isScattered();
   }

   @Override
   public void handle(CacheRpcCommand command, Reply reply, DeliverOrder order) {
      if (order == DeliverOrder.TOTAL) {
         unexpectedDeliverMode(command, order);
      }
      try {
         if (isCommandSentBeforeFirstTopology(command)) {
            reply.reply(CacheNotFoundResponse.INSTANCE);
            return;
         }

         final boolean onExecutorService = executeOnExecutorService(order, command);
         final boolean sync = order.preserveOrder();
         final BlockingRunnable runnable;

         switch (command.getCommandId()) {
            case SingleRpcCommand.COMMAND_ID:
               runnable =
                     createReadyActionRunnable(command, reply, onExecutorService, sync, createReadyAction( (SingleRpcCommand) command)) ;
               break;
            default:
               runnable = createDefaultRunnable(command, reply, sync);
               break;
         }
         handleRunnable(runnable, onExecutorService);
      } catch (Throwable throwable) {
         reply.reply(exceptionHandlingCommand(command, throwable));
      }
   }

   @Override
   public void onEvent(LockState state) {
      remoteCommandsExecutor.checkForReadyTasks();
   }

   @Override
   protected Log getLog() {
      return log;
   }

   @Override
   protected boolean isTraceEnabled() {
      return trace;
   }

   private BlockingRunnable createReadyActionRunnable(CacheRpcCommand command, Reply reply, boolean onExecutorService,
         boolean sync, ReadyAction readyAction) {
      if (onExecutorService && readyAction != null) {
         readyAction.addListener(remoteCommandsExecutor::checkForReadyTasks);
         return new DefaultTopologyRunnable(this, command, reply, sync) {
            @Override
            public boolean isReady() {
               return super.isReady() && readyAction.isReady();
            }

            @Override
            protected void onException(Throwable throwable) {
               super.onException(throwable);
               readyAction.onException();
            }

            @Override
            protected void onFinally() {
               super.onFinally();
               readyAction.onFinally();
            }
         };
      } else {
         return new DefaultTopologyRunnable(this, command, reply, sync);
      }
   }

   private ReadyAction createReadyAction(RemoteLockCommand command) {
   private ReadyAction createReadyAction(int topologyId, RemoteLockCommand command) {
      if (command.hasSkipLocking() || !isLocking) {
         return null;
      }
      Collection<?> keys = command.getKeysToLock();
      if (keys.isEmpty()) {
         return null;
      }
      final long timeoutMillis = command.hasZeroLockAcquisition() ? 0 : lockTimeout;

      DefaultReadyAction action = new DefaultReadyAction(new ActionState(command,  timeoutMillis),

                                                         new LockAction(lockManager, clusteringDependentLogic));
      action.registerListener();
      return action;
   }

   private ReadyAction createReadyAction(SingleRpcCommand singleRpcCommand) {
      ReplicableCommand command = singleRpcCommand.getCommand();
      return command instanceof RemoteLockCommand ?
            createReadyAction((RemoteLockCommand & ReplicableCommand) command) :
            null;
   }
}
