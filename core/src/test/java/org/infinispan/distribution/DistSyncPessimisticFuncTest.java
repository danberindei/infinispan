package org.infinispan.distribution;

import org.infinispan.Cache;
import org.infinispan.context.Flag;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

@Test(groups = "functional", testName = "distribution.DistSyncL1PessimisticFuncTest")
public class DistSyncPessimisticFuncTest extends BaseDistFunctionalTest {

   public DistSyncPessimisticFuncTest() {
      sync = true;
      tx = true;
      testRetVals = true;
      l1CacheEnabled = false;
      lockingMode = LockingMode.PESSIMISTIC;
      lockTimeout = 1;
   }

   public void testRemoteGetForceWriteLock() throws Exception {
      final String key = "some-key";
      String value = "some-value";
      final String otherValue = "some-new-value";

      final Cache<Object, String> nonOwner = getFirstNonOwner(key);
      final Cache<Object, String> primaryOwner = getFirstOwner(key);

      primaryOwner.put(key, value);

      // Lock the key with a read from a non-owner
      Transaction nonOwnerTx = null;
      TransactionManager nonOwnerTM = TestingUtil.getTransactionManager(nonOwner);
      nonOwnerTM.begin();
      try {
         nonOwner.getAdvancedCache().withFlags(Flag.FORCE_WRITE_LOCK).get(key);

         // Check that the key is locked on the primary
         assertTrue(primaryOwner.getAdvancedCache().getLockManager().isLocked(key));

         nonOwnerTx = nonOwnerTM.suspend();

         // Now try to write from the primary owner
         TransactionManager ownerTM = TestingUtil.getTransactionManager(primaryOwner);
         ownerTM.begin();
         try {
            // This should lock the key
            primaryOwner.put(key, otherValue);
            ownerTM.commit();
            fail("Should not be able to acquire the lock");
         } catch (Exception e) {
            log.tracef("Caught expected exception %s", e);
            ownerTM.rollback();
         }
      } finally {
         nonOwnerTM.resume(nonOwnerTx);
         nonOwnerTM.commit();
      }
   }
}
