package org.infinispan.commons.executors;

import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Default thread factory for executors.
 * Always sets the thread context classloader to {@code null}.
 *
 * @author Dan Berindei
 * @since 7.0
 */
public class DefaultWorkerThreadFactory implements ThreadFactory {
   private final String threadNamePrefix;
   private final String threadNameSuffix;
   private final int priority;
   private final AtomicInteger counter;
   private final ClassLoader contextClassLoader;
   private final AccessControlContext context;

   public DefaultWorkerThreadFactory(String threadName, AccessControlContext context, ClassLoader contextClassLoader) {
      this(threadName, null, "", context, contextClassLoader);
   }
   public DefaultWorkerThreadFactory(String threadNamePrefix, AtomicInteger counter,
                                     String threadNameSuffix, AccessControlContext context,
                                     ClassLoader contextClassLoader) {
      this(threadNamePrefix, counter, threadNameSuffix, Thread.NORM_PRIORITY, context, contextClassLoader);
   }

   public DefaultWorkerThreadFactory(String threadNamePrefix, AtomicInteger counter,
                                     String threadNameSuffix, int priority,
                                     AccessControlContext context,
                                     ClassLoader contextClassLoader) {
      this.threadNamePrefix = threadNamePrefix;
      this.threadNameSuffix = threadNameSuffix;
      this.priority = priority;
      this.counter = counter;
      this.context = context;
      this.contextClassLoader = contextClassLoader;
   }

   private Thread createThread(Runnable r) {
      StringBuilder builder = new StringBuilder(threadNamePrefix);
      if (counter != null) {
         builder.append(counter.getAndIncrement());
      }
      builder.append(threadNameSuffix);
      String threadName = builder.toString();

      Thread thread = new Thread(r, threadName);
      thread.setDaemon(true);
      thread.setPriority(priority);
      thread.setContextClassLoader(contextClassLoader);
      return thread;
   }

   public Thread newThread(Runnable r) {
      final Runnable runnable = r;
      final AccessControlContext acc;
      if (System.getSecurityManager() != null && (acc = context) != null) {
         return AccessController.doPrivileged(new PrivilegedAction<Thread>() {
            @Override
            public Thread run() {
               return createThread(runnable);
            }
         }, acc);
      } else {
         return createThread(runnable);
      }
   }
}
