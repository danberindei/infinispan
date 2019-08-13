package org.infinispan.distribution.ch.impl;

import static java.lang.Integer.toUnsignedLong;

public class MagicDivision {
   public static int div(int n, int magicMultiplier, int magicShift) {
      return (int)((n * toUnsignedLong(magicMultiplier)) >> magicShift);
   }

   /**
    * Compute a magic number to perform the division by {@code d} via multiplication + shift.
    *
    * <p>Adapted from Hacker's Delight</p>
    *
    * @param d Must have {@code 1 <= d <= 2**31-1}
    * @return An array {@code [magicMultiplier, magicShift]}
    */
   public static int[] computeMagicNumber(int d) {
      if (d < 1)
         throw new IllegalStateException("Divisor must be between 1 and 2**31-1");

      int nbits = 31;
      long nc = ((1L << nbits) / d) * d - 1;
      for (int p = 0; p < 2 * nbits + 1; p++) {
         long pow_2_p = 1L << p;
         if (pow_2_p > nc * (d - 1 - (pow_2_p - 1) % d)) {
            int m = (int)((pow_2_p + d - 1 - (pow_2_p - 1) % d) / d);
            return new int[]{m, p};
         }
      }
      throw new IllegalStateException("Magic number not found for " + d);
   }
}
