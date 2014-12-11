package org.infinispan.distribution.rehash;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.statetransfer.StateConsumer;
import org.infinispan.statetransfer.StateProvider;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.concurrent.InvocationMatcher;
import org.infinispan.test.concurrent.StateSequencer;
import org.infinispan.topology.ClusterTopologyManager;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.impl.RemoteTransaction;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.util.ControlledConsistentHashFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Collections;

import static org.infinispan.test.concurrent.StateSequencerUtil.advanceOnComponentMethod;
import static org.infinispan.test.concurrent.StateSequencerUtil.advanceOnGlobalComponentMethod;
import static org.infinispan.test.concurrent.StateSequencerUtil.matchMethodCall;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

/**
 * Tests that state transfer properly replicates locks in a pessimistic cache, when the
 * originator of the transaction is/was the primary owner.
 *
 * See ISPN-4091, ISPN-4108
 *
 * @author Dan Berindei
 * @since 7.0
 */
@Test(groups = "functional", testName = "distribution.rehash.PessimisticStateTransferLocksTest")
public class PessimisticStateTransferLocksTest extends MultipleCacheManagersTest {

   private static final String KEY = "key";
   private static final String VALUE = "value";

   {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   private StateSequencer sequencer;
   private ControlledConsistentHashFactory consistentHashFactory;

   @DataProvider(name = "newPrimaryWasBackup")
   public static Object[][] provideNewPrimaryWasBackup() {
      return new Object[][]{{false}, {true}};
   }

   @AfterMethod(alwaysRun = true)
   public void printSequencerState() {
      log.debugf("Sequencer state: %s", sequencer);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder c = getConfigurationBuilder();

      addClusterEnabledCacheManager(c);
      addClusterEnabledCacheManager(c);
      addClusterEnabledCacheManager(c);
      waitForClusterToForm();
   }

   @Test(dataProvider = "newPrimaryWasBackup")
   protected ConfigurationBuilder getConfigurationBuilder() {
      consistentHashFactory = new ControlledConsistentHashFactory(0, 1);
      ConfigurationBuilder c = new ConfigurationBuilder();
      c.clustering().cacheMode(CacheMode.DIST_SYNC);
      c.clustering().hash().consistentHashFactory(consistentHashFactory).numSegments(1);
      c.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      c.transaction().lockingMode(LockingMode.PESSIMISTIC);
      return c;
   }

   @Test(dataProvider = "newPrimaryWasBackup")
   public void testPutStartedBeforeRebalance(boolean newPrimaryWasBackup) throws Exception {
      sequencer = new StateSequencer();
      sequencer.logicalThread("tx", "tx:perform_op", "tx:check_locks", "tx:before_commit", "tx:after_commit");
      sequencer.logicalThread("rebalance", "rebalance:before_get_tx", "rebalance:after_get_tx",
            "rebalance:before_confirm", "rebalance:end");
      sequencer.order("tx:perform_op", "rebalance:before_get_tx", "rebalance:after_get_tx", "tx:check_locks",
            "rebalance:before_confirm", "rebalance:end", "tx:before_commit");

      startTxWithPut();
      startRebalance(newPrimaryWasBackup);
      checkLocksBeforeCommit(newPrimaryWasBackup, false);
      waitRebalanceEnd();
      endTx();
      checkLocksAfterCommit();
   }

   @Test(dataProvider = "newPrimaryWasBackup")
   public void testLockStartedBeforeRebalance(boolean newPrimaryWasBackup) throws Exception {
      sequencer = new StateSequencer();
      sequencer.logicalThread("tx", "tx:perform_op", "tx:check_locks", "tx:before_commit", "tx:after_commit");
      sequencer.logicalThread("rebalance", "rebalance:before_get_tx", "rebalance:after_get_tx",
            "rebalance:before_confirm", "rebalance:end");
      sequencer.order("tx:perform_op", "rebalance:before_get_tx", "rebalance:after_get_tx", "tx:check_locks",
            "rebalance:before_confirm", "rebalance:end", "tx:before_commit");

      startTxWithLock();
      startRebalance(newPrimaryWasBackup);
      checkLocksBeforeCommit(newPrimaryWasBackup, false);
      waitRebalanceEnd();
      endTx();
      checkLocksAfterCommit();
   }

   @Test(dataProvider = "newPrimaryWasBackup")
   public void testPutStartedDuringRebalance(boolean newPrimaryWasBackup) throws Exception {
      sequencer = new StateSequencer();
      sequencer.logicalThread("tx", "tx:perform_op", "tx:check_locks", "tx:before_commit",
            "tx:after_commit");
      sequencer.logicalThread("rebalance", "rebalance:before_get_tx", "rebalance:after_get_tx",
            "rebalance:before_confirm", "rebalance:end");
      sequencer.order("rebalance:after_get_tx", "tx:perform_op", "tx:check_locks",
            "rebalance:before_confirm", "rebalance:end", "tx:before_commit");

      startRebalance(newPrimaryWasBackup);
      startTxWithPut();
      checkLocksBeforeCommit(newPrimaryWasBackup, true);
      waitRebalanceEnd();
      endTx();
      checkLocksAfterCommit();
   }

   @Test(dataProvider = "newPrimaryWasBackup")
   public void testLockStartedDuringRebalance(boolean newPrimaryWasBackup) throws Exception {
      sequencer = new StateSequencer();
      sequencer.logicalThread("tx", "tx:perform_op", "tx:check_locks", "tx:before_commit", "tx:after_commit");
      sequencer.logicalThread("rebalance", "rebalance:before_get_tx", "rebalance:after_get_tx",
            "rebalance:before_confirm", "rebalance:end");
      sequencer.order("rebalance:after_get_tx", "tx:perform_op",  "tx:check_locks",
            "rebalance:before_confirm", "rebalance:end", "tx:before_commit");

      startRebalance(newPrimaryWasBackup);
      startTxWithLock();
      checkLocksBeforeCommit(newPrimaryWasBackup, true);
      waitRebalanceEnd();
      endTx();
      checkLocksAfterCommit();
   }

   private void startTxWithPut() throws Exception {
      sequencer.enter("tx:perform_op");
      tm(0).begin();
      cache(0).put(KEY, VALUE);
      sequencer.exit("tx:perform_op");
   }

   private void startTxWithLock() throws Exception {
      sequencer.enter("tx:perform_op");
      tm(0).begin();
      advancedCache(0).lock(KEY);
      sequencer.exit("tx:perform_op");
   }

   private void startRebalance(boolean newPrimaryWasBackup) throws Exception {
      InvocationMatcher rebalanceCompletedMatcher = matchMethodCall("handleRebalanceCompleted")
            .withParam(1, address(2)).build();
      advanceOnGlobalComponentMethod(sequencer, manager(0), ClusterTopologyManager.class,
            rebalanceCompletedMatcher).before("rebalance:before_confirm");

      InvocationMatcher localRebalanceMatcher = matchMethodCall("onTopologyUpdate").withParam(1, true).build();
      advanceOnComponentMethod(sequencer, cache(2), StateConsumer.class,
            localRebalanceMatcher).before("rebalance:before_get_tx").after("rebalance:after_get_tx");
      int[] ownerIndexes = newPrimaryWasBackup ? new int[]{1, 2} : new int[]{2, 1};
      consistentHashFactory.setOwnerIndexes(ownerIndexes);
      consistentHashFactory.triggerRebalance(cache(0));
   }

   private void waitRebalanceEnd() throws Exception {
      sequencer.advance("rebalance:end");
      TestingUtil.waitForRehashToComplete(caches());
   }

   private void endTx() throws Exception {
      sequencer.advance("tx:before_commit");
      tm(0).commit();
   }

   private void checkLocksBeforeCommit(boolean newPrimaryWasBackup, boolean backupLock) throws Exception {
      int oldPrimaryIndex = 0;
      int newPrimaryIndex = newPrimaryWasBackup ? 1 : 2;
      int newBackupIndex = newPrimaryWasBackup ? 2 : 1;

      sequencer.enter("tx:check_locks");
      assertFalse(getTransactionTable(cache(oldPrimaryIndex)).getLocalTransactions().isEmpty());
      assertTrue(getTransactionTable(cache(oldPrimaryIndex)).getRemoteTransactions().isEmpty());
      LocalTransaction localTx = getTransactionTable(cache(oldPrimaryIndex)).getLocalTransactions().iterator().next();
      assertEquals(Collections.singleton(KEY), localTx.getLockedKeys());
      assertEquals(Collections.emptySet(), localTx.getBackupLockedKeys());

      assertTrue(getTransactionTable(cache(newPrimaryIndex)).getLocalTransactions().isEmpty());
      assertFalse(getTransactionTable(cache(newPrimaryIndex)).getRemoteTransactions().isEmpty());
      RemoteTransaction remoteTx = getTransactionTable(cache(newPrimaryIndex)).getRemoteTransactions().iterator().next();
      assertEquals(Collections.emptySet(), remoteTx.getLockedKeys());
      assertEquals(Collections.singleton(KEY), remoteTx.getBackupLockedKeys());

      assertTrue(getTransactionTable(cache(newBackupIndex)).getLocalTransactions().isEmpty());
      assertEquals(backupLock, !getTransactionTable(cache(newBackupIndex)).getRemoteTransactions().isEmpty());

      //TODO Try another transaction with a write here and expect a timeout?

      sequencer.exit("tx:check_locks");
   }

   private void checkLocksAfterCommit() {
      for (Cache<Object, Object> c : caches()) {
         final TransactionTable txTable = getTransactionTable(c);
         assertTrue(txTable.getLocalTransactions().isEmpty());
         eventually(new Condition() {
            @Override
            public boolean isSatisfied() throws Exception {
               return txTable.getRemoteTransactions().isEmpty();
            }
         });
      }
   }

   private TransactionTable getTransactionTable(Cache<Object, Object> c) {
      return TestingUtil.extractComponent(c, TransactionTable.class);
   }
}