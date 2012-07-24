package org.infinispan.tx.totalorder;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.versioning.VersionedDistStateTransferTest;
import org.infinispan.transaction.TransactionProtocol;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test (groups = "functional", testName = "tx.totalorder.DistTotalOrderVersionedStateTransferTest")
public class DistTotalOrderVersionedStateTransferTest extends VersionedDistStateTransferTest {

   @Override
   protected void amendConfig(ConfigurationBuilder dcc) {
      dcc.transaction().transactionProtocol(TransactionProtocol.TOTAL_ORDER);
   }
}
