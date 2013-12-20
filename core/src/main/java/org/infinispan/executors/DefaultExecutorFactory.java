package org.infinispan.executors;

import org.infinispan.commons.executors.AdvancedExecutorFactory;
import org.infinispan.commons.executors.DefaultWorkerThreadFactory;
import org.infinispan.commons.executors.SecurityAwareExecutorFactory;
import org.infinispan.commons.util.TypedProperties;

import java.security.AccessControlContext;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Default executor factory that creates executors using the JDK Executors service.
 *
 * @author Manik Surtani
 * @author Tristan Tarrant
 * @since 4.0
 */
public class DefaultExecutorFactory implements SecurityAwareExecutorFactory, AdvancedExecutorFactory {
   private final AtomicInteger counter = new AtomicInteger(0);

   @Override
   public ExecutorService getExecutor(Properties p) {
      return getExecutor(p, Thread.currentThread().getContextClassLoader());
   }

   @Override
   public ExecutorService getExecutor(Properties p, ClassLoader classLoader) {
      return getExecutor(p, null, classLoader);
   }

   @Override
   public ExecutorService getExecutor(Properties p, AccessControlContext context) {
      return getExecutor(p, context, null);
   }
   
   public ExecutorService getExecutor(Properties p, AccessControlContext context, ClassLoader classLoader) {
      TypedProperties tp = TypedProperties.toTypedProperties(p);
      int maxThreads = tp.getIntProperty("maxThreads", 1);
      int queueSize = tp.getIntProperty("queueSize", 100000);
      int coreThreads = queueSize == 0 ? 1 : tp.getIntProperty("coreThreads", maxThreads);
      long keepAliveTime = tp.getLongProperty("keepAliveTime", 60000);
      int threadPrio = tp.getIntProperty("threadPriority", Thread.MIN_PRIORITY);
      String threadNamePrefix = tp.getProperty("threadNamePrefix", tp.getProperty("componentName", "Thread"));
      String threadNameSuffix = tp.getProperty("threadNameSuffix", "");
      BlockingQueue<Runnable> queue = queueSize == 0 ? new SynchronousQueue<Runnable>() :
            new LinkedBlockingQueue<Runnable>(queueSize);
      ThreadFactory tf = new DefaultWorkerThreadFactory(threadNamePrefix + "-", counter, threadNameSuffix, threadPrio, context, classLoader);

      return new ThreadPoolExecutor(coreThreads, maxThreads, keepAliveTime, TimeUnit.MILLISECONDS, queue, tf,
            new ThreadPoolExecutor.CallerRunsPolicy());
   }
}
