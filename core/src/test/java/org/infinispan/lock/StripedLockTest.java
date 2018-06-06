package org.infinispan.lock;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.util.concurrent.locks.StripedLock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tester class for {@link org.infinispan.util.concurrent.locks.StripedLock}.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "unit", testName = "lock.StripedLockTest")
public class StripedLockTest extends AbstractInfinispanTest {

   StripedLock stripedLock;

   /* this value will make sure that the index of the underlying shared lock is not 0*/
   private static final String KEY = "21321321321321321";


   @BeforeMethod
   public void cretateStripedLock() {
      stripedLock = new StripedLock(5);
   }

   public void testGlobalReadLockSimple() throws Exception {
      assert canAquireWL();
      assert canAquireRL();
      assert stripedLock.acquireGlobalLock(false, 0);
      assert stripedLock.getTotalReadLockCount() == stripedLock.getSharedLockCount();
      assert !canAquireWL();
      assert canAquireRL();
   }

   public void testGlobalReadLockIsAtomic() throws Exception {
      assert aquireWL();
      assert 1 == stripedLock.getTotalWriteLockCount();
      assert !stripedLock.acquireGlobalLock(false, 0);
      assert stripedLock.getTotalReadLockCount() == 0 : "No read locks should be held if the operation failed";
   }

   public void testGlobalReadLockOverExistingReadLocks() throws Exception {
      assert aquireRL();
      assert aquireRL();
      assert stripedLock.getTotalReadLockCount() == 2;
      assert stripedLock.acquireGlobalLock(false, 0);
      assert stripedLock.getTotalReadLockCount() == stripedLock.getSharedLockCount() + 2;
   }

   public void testAquireGlobalAndRelease() {
      assert stripedLock.acquireGlobalLock(false, 0);
      assert stripedLock.getTotalReadLockCount() == stripedLock.getSharedLockCount();
      assert stripedLock.getTotalWriteLockCount() == 0;
      try {
         stripedLock.releaseGlobalLock(true); //this should not fail
         assert false : "this should fail as we do not have a monitor over the locks";
      } catch (Exception e) {
         //expected
      }
      stripedLock.releaseGlobalLock(false);
      assert stripedLock.getTotalReadLockCount() == 0;
      assert stripedLock.getTotalWriteLockCount() == 0;

      assert stripedLock.acquireGlobalLock(true, 0);
      assert stripedLock.getTotalReadLockCount() == 0;
      assert stripedLock.getTotalWriteLockCount() == stripedLock.getSharedLockCount();

      try {
         stripedLock.releaseGlobalLock(false); //this should not fail
         assert false : "this should fail as we do not have a monitor over the locks";
      } catch (Exception e) {
         //expected
      }
      stripedLock.releaseGlobalLock(true);
      assert stripedLock.getTotalReadLockCount() == 0;
      assert stripedLock.getTotalWriteLockCount() == 0;
   }

   private boolean aquireWL() throws Exception {
      Future<Boolean> fork = fork(() -> stripedLock.acquireLock(KEY, true, 0));
      return fork.get(10, TimeUnit.SECONDS);
   }

   private boolean aquireRL() throws Exception {
      Future<Boolean> fork = fork(() -> stripedLock.acquireLock(KEY, false, 0));
      return fork.get(10, TimeUnit.SECONDS);
   }

   private boolean canAquireRL() throws Exception {
      Future<Boolean> fork = fork(() -> {
         Boolean response = stripedLock.acquireLock(KEY, false, 0);
         if (response) stripedLock.releaseLock(KEY);
         return response;
      });
      return fork.get(10, TimeUnit.SECONDS);
   }

   private boolean canAquireWL() throws Exception {
      Future<Boolean> fork = fork(() -> {
         Boolean response = stripedLock.acquireLock(KEY, true, 0);
         if (response) stripedLock.releaseLock(KEY);
         return response;
      });
      return fork.get(10, TimeUnit.SECONDS);
   }

}
