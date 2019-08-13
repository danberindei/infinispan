package org.infinispan.distribution.ch.impl;

import static org.testng.AssertJUnit.assertEquals;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "distribution.ch.impl.MagicDivisionTest")
public class MagicDivisionTest {
   private int[] DIVIDENDS = {1, 2, 10, 100, 1000, 1_000_000, 1_000_000_000, Integer.MAX_VALUE};

   @DataProvider(name = "divisors")
   public Object[][] divisors() {
      return new Object[][]{{1}, {2}, {3}, {4}, {5}, {7}, {255}, {256}, {257},
                            {1_234}, {1_234_567}, {1_234_567_890}, {Integer.MAX_VALUE}};
   }

   @Test(dataProvider = "divisors")
   public void testMagicDiv(int divisor) {
      int[] magic = MagicDivision.computeMagicNumber(divisor);
      for (int dividend : DIVIDENDS) {
         check(dividend, divisor, magic);
      }
   }

   private void check(int dividend, int divisor, int[] magic) {
      int expected = dividend / divisor;
      int actual = MagicDivision.div(dividend, magic[0], magic[1]);
      assertEquals(expected, actual);
   }
}
