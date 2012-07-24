package org.infinispan.commands.remote;

import org.infinispan.context.InvocationContext;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.totalorder.TotalOrderManager;
import org.infinispan.transaction.xa.GlobalTransaction;

import java.util.Collection;

/**
 * This command is used in Total Order distributed mode when write skew check is enabled.
 * It contains the result of the write skew check, that can be an exception (write skew check failed) or a set of keys
 * which pass the validation in the sender node.
 * 
 * It passes this information to the total order manager.
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class PrepareResponseCommand extends BaseRpcCommand {

   public static final byte COMMAND_ID = 100;

   private Object result;
   private GlobalTransaction globalTransaction;
   private transient TotalOrderManager totalOrderManager;

   public PrepareResponseCommand(String cacheName) {
      super(cacheName);
   }

   public PrepareResponseCommand(String cacheName, GlobalTransaction globalTransaction) {
      super(cacheName);
      this.globalTransaction = globalTransaction;
   }
   
   public void initialize(TotalOrderManager totalOrderManager) {
      this.totalOrderManager = totalOrderManager;
   }
   
   public void addResult(Object result) {
      this.result = result;
   }

   @SuppressWarnings("unchecked")
   @Override
   public Object perform(InvocationContext context) throws Throwable {
      if (totalOrderManager == null) {
         throw new IllegalStateException("Total Order Manager cannot be null");
      }
      LocalTransaction localTransaction = totalOrderManager.getLocalTransaction(globalTransaction);
      if (localTransaction == null) {
         return null;
      }
      
      if (result instanceof Collection) {
         localTransaction.addKeysValidated((Collection<Object>) result, false);
      } else if (result instanceof Exception) {
         localTransaction.addException((Exception) result, false);
      } else {
         throw new IllegalStateException("Unknown Prepare result value. Expected a collection or an exception but " +
                                               "received " + result.getClass());
      }
      return null;
   }

   @Override
   public Object[] getParameters() {
      return new Object[]{globalTransaction, result};
   }

   @SuppressWarnings({"unchecked"})
   @Override
   public void setParameters(int commandId, Object[] args) {
      this.globalTransaction = (GlobalTransaction) args[0];
      this.result = args[1];
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public String toString() {
      return "PrepareResponseCommand{" +
            "globalTransaction=" + globalTransaction +
            ", result=" + result +
            ", cacheName" + cacheName +
            "}";
   }
}
