package org.infinispan.client.hotrod;

import java.util.concurrent.TimeUnit;

import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.BaseCustomAsyncInterceptor;
import org.infinispan.marshall.core.JBossMarshaller;
import org.infinispan.test.AbstractInfinispanTest;

public class TimeoutInducingInterceptor extends BaseCustomAsyncInterceptor {

   private final AbstractInfinispanTest test;

   public TimeoutInducingInterceptor(AbstractInfinispanTest test) {
      this.test = test;
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      if (unmarshall(command.getKey()).equals("FailFailFail")) {
         return asyncValue(test.delayed(null, 6000, TimeUnit.MILLISECONDS));
      }

      return super.visitPutKeyValueCommand(ctx, command);
   }

   private String unmarshall(Object key) throws Exception {
      Marshaller marshaller = new JBossMarshaller();
      return (String) marshaller.objectFromByteBuffer(((WrappedByteArray) key).getBytes());
   }
}
