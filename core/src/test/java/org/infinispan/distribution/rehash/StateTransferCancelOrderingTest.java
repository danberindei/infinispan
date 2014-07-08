package org.infinispan.distribution.rehash;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.statetransfer.StateProvider;
import org.infinispan.statetransfer.StateResponseCommand;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.concurrent.StateSequencer;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.ControlledConsistentHashFactory;
import org.testng.annotations.Test;

import java.util.Arrays;

import static org.infinispan.test.TestingUtil.waitForRehashToComplete;
import static org.infinispan.test.concurrent.StateSequencerUtil.advanceOnComponentMethod;
import static org.infinispan.test.concurrent.StateSequencerUtil.advanceOnOutboundRpc;
import static org.infinispan.test.concurrent.StateSequencerUtil.matchCommand;
import static org.infinispan.test.concurrent.StateSequencerUtil.matchMethodCall;
import static org.testng.Assert.assertNotNull;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

/**
 * Stop the coordinator of a cluster and allow it to start the rebalance but not finish.
 * The new coordinator will cancel the rebalance and start a new one.
 * Test that the state transfer cancel commands for the old topology do not interfere with the new state transfer.
 * See https://issues.jboss.org/browse/ISPN-4484
 *
 * @author Dan Berindei
 * @since 7.0
 */
@CleanupAfterMethod
@Test(groups = "functional", testName = "distribution.rehash.StateTransferCancelOrderingTest")
public class StateTransferCancelOrderingTest extends MultipleCacheManagersTest {

   private ControlledConsistentHashFactory consistentHashFactory;

   @Override
   protected void createCacheManagers() throws Throwable {
      consistentHashFactory = new ControlledConsistentHashFactory(new int[]{0, 1});
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      builder.clustering().cacheMode(CacheMode.DIST_SYNC);
      builder.clustering().hash().numOwners(2).numSegments(1).consistentHashFactory(consistentHashFactory);
      createCluster(builder, 3);
      waitForClusterToForm();
   }

   public void testLateCancelCommand() throws Throwable {
      // Initial owners are cache 0 and cache 1
      // Cache 0 leaves, ClusterTopologyManager on node 0 starts a rebalance
      // Cache 1 receives a StateRequestCommand(START_STATE_TRANSFER) from cache 2,
      // Cache 1 sends state to cache 2 and blocks before marking the state transfer as done
      // Node 0 dies, node 1 becomes coordinator and cancels the rebalance
      // Cache 1 receives a StateRequestCommand(CANCEL_STATE_TRANSFER) from cache 2, which also blocks
      // Cache 1 receives a StateRequestCommand(START_STATE_TRANSFER) for the new topology
      // Blocks the StateResponseCommand and unblocks the old state transfer and cancel command
      StateSequencer sequencer = new StateSequencer();
      sequencer.logicalThread("main", "main:before_stop_node0", "main:resume_st1_cancel");
      sequencer.logicalThread("st1", "st1:before_state_response", "st1:resume_before_state_response");
      sequencer.logicalThread("cancel", "cancel:before_cancel", "cancel:resume_before_cancel", "cancel:after_cancel");
      sequencer.logicalThread("st2", "st2:before_state_response", "st2:resume_before_state_response");
      sequencer.order("st1:before_state_response", "main:before_stop_node0", "cancel:before_cancel",
            "st2:before_state_response", "main:resume_st1_cancel", "cancel:resume_before_cancel");
      // The outbound transfer task will be cancelled, so st1:resume_before_state_response will never be exited
      sequencer.order("main:resume_st1_cancel", "st1:resume_before_state_response");
      sequencer.order("cancel:after_cancel", "st2:resume_before_state_response");

      cache(0).put("k1", "v1");
      cache(0).put("k2", "v2");
      cache(0).put("k3", "v3");

      final StateTransferManager stm1 = advancedCache(1).getComponentRegistry().getStateTransferManager();

      assertEquals(Arrays.asList(address(0), address(1)), stm1.getCacheTopology().getCurrentCH().locateOwnersForSegment(0));
      assertNull(stm1.getCacheTopology().getPendingCH());

      // Block after cache 1 sends the 1st state response to cache 2
      advanceOnOutboundRpc(sequencer, cache(1), matchCommand(StateResponseCommand.class).matchCount(0).build())
            .before("st1:before_state_response", "st1:resume_before_state_response");
      // Block before cache 1 sends the 2nd state response to cache 2
      advanceOnOutboundRpc(sequencer, cache(1), matchCommand(StateResponseCommand.class).matchCount(1).build())
            .before("st2:before_state_response", "st2:resume_before_state_response");
      // Block before executing the cancel command
      advanceOnComponentMethod(sequencer, cache(1), StateProvider.class, matchMethodCall("cancelOutboundTransfer").build())
            .before("cancel:before_cancel", "cancel:resume_before_cancel").after("cancel:after_cancel");

      // Cache 2 will become an owner and will request state from cache 1
      cache(0).stop();

      sequencer.advance("main:before_stop_node0");

      assertEquals(Arrays.asList(address(1)), stm1.getCacheTopology().getCurrentCH().locateOwnersForSegment(0));
      assertNotNull(stm1.getCacheTopology().getPendingCH());
      assertEquals(Arrays.asList(address(1), address(2)), stm1.getCacheTopology().getPendingCH().locateOwnersForSegment(0));

      manager(0).stop();

      sequencer.advance("main:resume_st1_cancel");

      // Wait for the rebalance to complete
      waitForRehashToComplete(cache(1), cache(2));

      assertEquals(Arrays.asList(address(1), address(2)), stm1.getCacheTopology().getCurrentCH().locateOwnersForSegment(0));
      assertNull(stm1.getCacheTopology().getPendingCH());

      // Check that state wasn't lost
      assertEquals("v1", cache(2).get("k1"));
      assertEquals("v2", cache(2).get("k2"));
      assertEquals("v3", cache(2).get("k3"));
   }
}
