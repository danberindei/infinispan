package org.infinispan.executors;

import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Used to configure and create scheduled executors
 *
 * Unlike {@link ScheduledExecutorFactory}, it also guarantees that the worker threads created by
 * its executors have their context classloader set to the {@code classLoader} parameter.
 *
 * @author Dan Berindei
 * @since 7.0
 */
public interface AdvancedScheduledExecutorFactory {
   ScheduledExecutorService getScheduledExecutor(Properties p, ClassLoader classLoader);
}
