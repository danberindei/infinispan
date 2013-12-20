package org.infinispan.commons.executors;

import java.util.Properties;
import java.util.concurrent.ExecutorService;

/**
 * Used to configure and create executors.
 *
 * Unlike {@link ExecutorFactory}, it also guarantees that the worker threads created by
 * its executors have their context classloader set to the {@code classLoader} parameter.
 *
 * @author Dan Berindei
 * @since 7.0
 */
public interface AdvancedExecutorFactory {
   ExecutorService getExecutor(Properties p, ClassLoader classLoader);
}
