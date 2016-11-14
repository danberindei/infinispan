package org.infinispan.remoting.inboundhandler;

import org.infinispan.IllegalLifecycleStateException;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.persistence.modifications.Remove;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.util.concurrent.BlockingRunnable;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Common logic to handle {@link org.infinispan.commands.remote.CacheRpcCommand}.
 *
 * @author Pedro Ruivo
 * @since 7.1
 */
public abstract class BaseBlockingRunnable implements BlockingRunnable {

   protected final BasePerCacheInboundInvocationHandler handler;
   protected final CacheRpcCommand command;
   protected final Reply reply;
   protected final boolean sync;
   protected Response response;

   protected BaseBlockingRunnable(BasePerCacheInboundInvocationHandler handler, CacheRpcCommand command, Reply reply,
                                  boolean sync) {
      this.handler = handler;
      this.command = command;
      this.reply = reply;
      this.sync = sync;
   }

   @Override
   public void run() {
      if (sync) {
         runSync();
      } else {
         runAsync();
      }
   }

   private void runSync() {
      try {
         response = beforeInvoke();
         if (response != null) {
               afterInvoke();return;

         }
         CompletableFuture<Response> commandFuture = handler.invokeCommand(command);
         response = commandFuture.join();
         afterInvoke();
      } catch (Throwable t) {
         afterCommandException(unwrap(t));
      } finally {
         if (handler.isStopped()) {
            response = CacheNotFoundResponse.INSTANCE;
         }
         reply.reply(response);
         onFinally();
      }
   }

   private void runAsync() {
      response = beforeInvoke();
      if (response == null) {
         invoke();
      } else {
         if (handler.isStopped()) {
            response = CacheNotFoundResponse.INSTANCE;
         }
         reply.reply(response);
         onFinally();
      }
   }

   private void invoke() {
      CompletableFuture<Response> commandFuture;
      try {
         commandFuture = handler.invokeCommand(command);
      } catch (Throwable t) {
         afterCommandException(unwrap(t));
         if (handler.isStopped()) {
            response = CacheNotFoundResponse.INSTANCE;
         }
         reply.reply(response);
         onFinally();
         return;
      }
      commandFuture.whenComplete((rsp, throwable) -> {
         try {
            if (throwable == null) {
               response = rsp;
               afterInvoke();
            } else {
               afterCommandException(unwrap(throwable));
            }
         } finally {
            if (handler.isStopped()) {
               response = CacheNotFoundResponse.INSTANCE;
            }
            reply.reply(response);
            onFinally();
         }
      });
   }

   private Throwable unwrap(Throwable throwable) {
      if (throwable instanceof CompletionException && throwable.getCause() != null) {
         throwable = throwable.getCause();
      }
      return throwable;
   }

   private void afterCommandException(Throwable throwable) {
      if (throwable instanceof InterruptedException) {
         response = handler.interruptedException(command);
      } else if (throwable instanceof OutdatedTopologyException) {
         response = handler.outdatedTopology((OutdatedTopologyException) throwable);
      } else if (throwable instanceof IllegalLifecycleStateException) {
         response = CacheNotFoundResponse.INSTANCE;
      } else {
         response = handler.exceptionHandlingCommand(command, throwable);
      }
      onException(throwable);
   }

   protected void onFinally() {
      //no-op by default
   }

   protected void onException(Throwable throwable) {
      //no-op by default
   }

   protected void afterInvoke() {
      //no-op by default
   }

   protected Response beforeInvoke() {
      return null; //no-op by default
   }

   @Override
   public String toString() {
      final StringBuilder sb = new StringBuilder(getClass().getSimpleName());
      sb.append("{command=").append(command);
      sb.append(", sync=").append(sync);
      sb.append('}');
      return sb.toString();
   }
}
