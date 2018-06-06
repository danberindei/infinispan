package org.infinispan.factories;

import static org.mockito.Mockito.mock;

import java.util.Collections;

import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Mock component registry that doesn't use any factories.
 *
 * @author Dan Berindei
 * @since 9.3
 */
public class MockGlobalComponentRegistry extends GlobalComponentRegistry {
   private static final Log log = LogFactory.getLog(MockGlobalComponentRegistry.class);

   public MockGlobalComponentRegistry(GlobalConfiguration gc) {
      super(gc, mock(EmbeddedCacheManager.class), Collections.emptySet());
   }

   @Override
   protected synchronized <T> T getOrCreateComponent(Class<T> componentClass, String name, boolean nameIsFQCN) {
      Component wrapper = lookupComponent(componentClass.getName(), name, nameIsFQCN);
      if (wrapper == null)
         return null;

      return componentClass.cast(unwrapComponent(wrapper));
   }

   @Override
   protected Log getLog() {
      return log;
   }
}
