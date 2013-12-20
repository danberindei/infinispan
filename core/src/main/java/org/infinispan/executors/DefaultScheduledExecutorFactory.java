package org.infinispan.executors;

import org.infinispan.commons.executors.DefaultWorkerThreadFactory;
import org.infinispan.commons.util.TypedProperties;

import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.commons.util.TypedProperties;

/**
 * Creates scheduled executors using the JDK Executors service
 *
 * @author Manik Surtani
 * @author Tristan Tarrant
 * @since 4.0
 */
public class DefaultScheduledExecutorFactory implements SecurityAwareScheduledExecutorFactory, AdvancedScheduledExecutorFactory {
   final static AtomicInteger counter = new AtomicInteger(0);

   @Override
   public ScheduledExecutorService getScheduledExecutor(Properties p) {
      return getScheduledExecutor(p, null, Thread.currentThread().getContextClassLoader());
   }

   @Override
   public ScheduledExecutorService getScheduledExecutor(Properties p, ClassLoader classLoader) {
      return getScheduledExecutor(p, null, classLoader);
   }

   @Override
   public ScheduledExecutorService getScheduledExecutor(Properties p, AccessControlContext context) {
      return getScheduledExecutor(p, context, Thread.currentThread().getContextClassLoader());
   }

   public ScheduledExecutorService getScheduledExecutor(Properties p, AccessControlContext context, ClassLoader classLoader) {
      TypedProperties tp = new TypedProperties(p);
      final String threadNamePrefix = p.getProperty("threadNamePrefix", p.getProperty("componentName", "Thread"));
      final int threadPrio = tp.getIntProperty("threadPriority", Thread.MIN_PRIORITY);

      ThreadFactory threadFactory = new DefaultWorkerThreadFactory("Scheduled-" + threadNamePrefix + "-", counter, "", threadPrio, context, classLoader);
      return Executors.newSingleThreadScheduledExecutor(threadFactory);
   }
}
