package org.infinispan.tx.totalorder;

import org.testng.annotations.Test;

/**
 * A simple test with the cache operations for total order based protocol in distributed mode, but with more cache
 * managers
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "tx.totalorder.DistSimpleTotalOrder1Pc2Test")
public class DistSimpleTotalOrder1Pc2Test extends DistSimpleTotalOrder1PcTest {

   public DistSimpleTotalOrder1Pc2Test() {
      this.clusterSize = 3;
   }
}
