package org.infinispan.client.hotrod.impl.async;

import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.commons.executors.DefaultWorkerThreadFactory;
import org.infinispan.commons.executors.ExecutorFactory;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Default implementation for {@link org.infinispan.commons.executors.ExecutorFactory} based on a {@link ThreadPoolExecutor}.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class DefaultAsyncExecutorFactory implements ExecutorFactory {
   public static final String THREAD_NAME = "HotRod-client-async-pool";
   public static final AtomicInteger counter = new AtomicInteger(0);

   @Override
   public ExecutorService getExecutor(Properties p) {
      ConfigurationProperties cp = new ConfigurationProperties(p);
      // We assume the client will have a single classloader
      ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
      ThreadFactory tf = new DefaultWorkerThreadFactory(THREAD_NAME + "-", counter, "", classLoader);

      return new ThreadPoolExecutor(cp.getDefaultExecutorFactoryPoolSize(), cp.getDefaultExecutorFactoryPoolSize(),
                                    0L, TimeUnit.MILLISECONDS,
                                    new LinkedBlockingQueue<Runnable>(cp.getDefaultExecutorFactoryQueueSize()),
                                    tf);
   }
}
