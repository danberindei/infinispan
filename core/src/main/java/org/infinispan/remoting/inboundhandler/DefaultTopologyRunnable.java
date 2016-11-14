package org.infinispan.remoting.inboundhandler;

import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.remoting.responses.Response;

/**
 * The default {@link Runnable} for the remote commands receives.
 * <p/>
 * It checks the command topology and ensures that the topology higher or equal is installed in this node.
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public class DefaultTopologyRunnable extends BaseBlockingRunnable {

   public DefaultTopologyRunnable(BasePerCacheInboundInvocationHandler handler, CacheRpcCommand command, Reply reply,
                                  boolean sync) {
      super(handler, command, reply, sync);
   }

   @Override
   public boolean isReady() {
      return true;
   }

   @Override
   protected Response beforeInvoke() {
      return null;
   }

   @Override
   public String toString() {
      final StringBuilder sb = new StringBuilder("DefaultTopologyRunnable{");
      sb.append(", command=").append(command);
      sb.append(", sync=").append(sync);
      sb.append('}');
      return sb.toString();
   }
}
