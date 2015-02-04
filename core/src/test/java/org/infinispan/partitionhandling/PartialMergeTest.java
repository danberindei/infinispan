package org.infinispan.partitionhandling;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.util.ControlledConsistentHashFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jgroups.Address;
import org.testng.annotations.Test;

import java.util.List;

/**
 * Test that when a partition
 */
@Test(groups = "functional", testName = "partitionhandling.DelayedAvailabilityUpdateTest")
public class PartialMergeTest extends BasePartitionHandlingTest {
   private static final Log log = LogFactory.getLog(PartialMergeTest.class);
   ControlledConsistentHashFactory cchf = new ControlledConsistentHashFactory(0, 2);

   @Override
   protected void amendCacheConfiguration(ConfigurationBuilder dcc) {
      dcc.clustering().hash().consistentHashFactory(cchf);
   }

   private void testDelayedAvailabilityUpdate() throws Exception {
      PartitionDescriptor p0 = new PartitionDescriptor(0, 2);
      PartitionDescriptor p1 = new PartitionDescriptor(1, 3);
      cchf.setMembersToUse(address(0), address(1), address(2), address(3));
      Object k1 = new MagicKey("k1", cache(0), cache(2));

      List<Address> allMembers = channel(0).getView().getMembers();
      Partition p = createPartition(allMembers, 0, 2);
   }
}
