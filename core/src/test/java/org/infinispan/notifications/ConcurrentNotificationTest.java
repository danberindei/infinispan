package org.infinispan.notifications;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.Cache;
import org.infinispan.manager.CacheContainer;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryVisited;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = {"functional", "smoke"}, testName = "notifications.ConcurrentNotificationTest")
public class ConcurrentNotificationTest extends AbstractInfinispanTest {
   Cache<String, String> cache;
   CacheContainer cm;
   CacheListener listener;
   Log log = LogFactory.getLog(ConcurrentNotificationTest.class);

   @BeforeMethod
   public void setUp() {
      cm = TestCacheManagerFactory.createCacheManager(false);
      cache = cm.getCache();
      listener = new CacheListener();
      cache.addListener(listener);
   }

   @AfterMethod
   public void tearDown() {
      TestingUtil.killCacheManagers(cm);
      cm = null;
      cache = null;
      listener = null;
   }

   public void testThreads() throws Exception {
      Future[] workers = new Future[20];
      final int loops = 100;
      final CountDownLatch latch = new CountDownLatch(1);

      for (int i = 0; i < workers.length; i++) {
         workers[i] = fork(() -> {
            latch.await();

            for (int j = 0; j < loops; j++) {
               cache.put("key", "value");

               cache.remove("key");

               cache.get("key");
            }
         });
      }

      latch.countDown();

      for (Future f : workers)
         f.get(30, TimeUnit.SECONDS);

      // we cannot ascertain the exact number of invocations on the replListener since some removes would mean that other
      // gets would miss.  And this would cause no notification to fire for that get.  And we cannot be sure of the
      // timing between removes and gets, so we just make sure *some* of these have got through, and no exceptions
      // were thrown due to concurrent access.
      assert loops * workers.length < listener.counter.get();
   }

   @Listener
   static public class CacheListener {
      private AtomicInteger counter = new AtomicInteger(0);

      @CacheEntryModified
      @CacheEntryRemoved
      @CacheEntryVisited
      @CacheEntryCreated
      public void catchEvent(Event e) {
         if (e.isPre())
            counter.getAndIncrement();
      }
   }
}
