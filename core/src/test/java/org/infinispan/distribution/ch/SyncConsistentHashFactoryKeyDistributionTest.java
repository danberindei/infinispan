package org.infinispan.distribution.ch;

import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.distribution.ch.impl.DefaultConsistentHash;
import org.infinispan.distribution.ch.impl.OwnershipStatistics;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.AbstractInfinispanTest;
import org.jgroups.util.UUID;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import static java.lang.Math.sqrt;
import static org.testng.AssertJUnit.assertEquals;

/**
 * Tests the uniformity of the SyncConsistentHashFactory algorithm, which is very similar to the 5.1
 * default consistent hash algorithm virtual nodes.
 *
 * <p>This test assumes that key hashes are random and follow a uniform distribution  so a key has the same chance
 * to land on each one of the 2^31 positions on the hash wheel.
 *
 * <p>The output should stay pretty much the same between runs, so I added and example output here: vnodes_key_dist.txt.
 *
 * <p>Notes about the test output:
 * <ul>
 * <li>{@code P(p)} is the probability of proposition {@code p} being true
 * <li>In the "Primary" rows {@code mean == total_keys / num_nodes} (each key has only one primary owner),
 * but in the "Any owner" rows {@code mean == total_keys / num_nodes * num_owners} (each key is stored on
 * {@code num_owner} nodes).
 * </ul>
 * @author Dan Berindei
 * @since 5.2
 */
@Test(testName = "distribution.ch.SyncConsistentHashFactoryKeyDistributionTest", groups = "profiling")
public class SyncConsistentHashFactoryKeyDistributionTest extends AbstractInfinispanTest {

   // numbers of nodes to test
   public static final int[] NUM_NODES = {10, 50, 100};
   // numbers of virtual nodes to test
   public static final int[] NUM_SEGMENTS = {128, 256, 512, 1024, 2048, 4096};
   // number of key owners
   public static final int NUM_OWNERS = 2;
   // if true, assign random capacity factors between 1 and 3
   public static final boolean RANDOM_CAPACITY_FACTORS = false;

   // controls precision + duration of test
   public static final int LOOPS = 200;
   // confidence intervals to print for any owner
   public static final double[] INTERVALS = { 0.9, 1.10 };
   // confidence intervals to print for primary owner
   public static final double[] INTERVALS_PRIMARY = { 0.9, 1.10 };
   // percentiles to print
   public static final double[] PERCENTILES = { .99 };

   private Random random = new Random();

   protected DefaultConsistentHash createConsistentHash(int numSegments, int numOwners, List<Address> members,
         Map<Address, Float> capacityFactors) {
      MurmurHash3 hash = MurmurHash3.getInstance();
      ConsistentHashFactory<DefaultConsistentHash> chf = new SyncConsistentHashFactory();
      DefaultConsistentHash ch = chf.create(hash, numOwners, numSegments, members, capacityFactors);
      return ch;
   }

   private Map<Address, Float> createCapacityFactors(List<Address> members) {
      Map<Address, Float> capacityFactors = new HashMap<>(members.size());
      if (RANDOM_CAPACITY_FACTORS) {
         for (Address member : members) {
            capacityFactors.put(member, (float) (Math.max(random.nextInt(3), 1)));
         }
      } else {
         for (Address member : members) {
            capacityFactors.put(member, 1.0f);
         }
      }
      return capacityFactors;
   }

   protected List<Address> createAddresses(int numNodes) {
      ArrayList<Address> addresses = new ArrayList<Address>(numNodes);
      for (int i = 0; i < numNodes; i++) {
         UUID address = UUID.randomUUID();
         addresses.add(new IndexedJGroupsAddress(address, i));
      }
      return addresses;
   }

   public void testDistribution() {
      int combinationIndex = 0;
      for (int nn : NUM_NODES) {
         Map<String, Map<Integer, String>> metrics = new TreeMap<String, Map<Integer, String>>();
         for (int ns : NUM_SEGMENTS) {
            for (Map.Entry<String, String> entry : computeMetrics(combinationIndex, ns, NUM_OWNERS, nn).entrySet()) {
               String metricName = entry.getKey();
               String metricValue = entry.getValue();
               Map<Integer, String> metric = metrics.get(metricName);
               if (metric == null) {
                  metric = new HashMap<Integer, String>();
                  metrics.put(metricName, metric);
               }
               metric.put(ns, metricValue);
               combinationIndex++;
            }
         }

         printMetrics(nn, metrics);
      }
   }

   private void printMetrics(int nn, Map<String, Map<Integer, String>> metrics) {
      // print the header
      System.out.printf("Distribution for %3d nodes (relative to the average)\n===\n", nn);
      System.out.printf("%30s = ", "Segments");
      for (int i = 0; i < NUM_SEGMENTS.length; i++) {
         System.out.printf("%7d", NUM_SEGMENTS[i]);
      }
      System.out.println();

      // print each metric for each vnodes setting
      for (Map.Entry<String, Map<Integer, String>> entry : metrics.entrySet()) {
         String metricName = entry.getKey();
         Map<Integer, String> metricValues = entry.getValue();

         System.out.printf("%30s = ", metricName);
         for (int i = 0; i < NUM_SEGMENTS.length; i++) {
            System.out.print(metricValues.get(NUM_SEGMENTS[i]));
         }
         System.out.println();
      }
      System.out.println();
   }

   private Map<String, String> computeMetrics(int combinationIndex, int numSegments, int numOwners, int numNodes) {
      Map<String, String> metrics = new HashMap<String, String>();
      double[] distribution = new double[LOOPS * numNodes];
      double[] distributionPrimary = new double[LOOPS * numNodes];
      int distIndex = 0;
      for (int i = 0; i < LOOPS; i++) {
         distIndex = doLoop(combinationIndex, numSegments, numOwners, numNodes, i, distribution, distributionPrimary,
               distIndex);
      }
      Arrays.sort(distribution);
      Arrays.sort(distributionPrimary);

      addMetrics(metrics, "Any owner:", numSegments, numOwners, numNodes, distribution, INTERVALS);
      addMetrics(metrics, "Primary:", numSegments, 1, numNodes, distributionPrimary, INTERVALS_PRIMARY);
      return metrics;
   }

   private int doLoop(int combinationIndex, int numSegments, int numOwners, int numNodes, int loopIndex,
         double[] distribution, double[] distributionPrimary, int distIndex) {
      List<Address> members = createAddresses(numNodes);
      Map<Address, Float> capacityFactors = createCapacityFactors(members);
      log.tracef("Combination %s loop %s capacity factors %s", combinationIndex, loopIndex, capacityFactors);
      DefaultConsistentHash ch = createConsistentHash(numSegments, numOwners, members, capacityFactors);
      log.tracef("Consistent hash: %s", ch);
      OwnershipStatistics stats = new OwnershipStatistics(ch, ch.getMembers());
      assertEquals(numSegments * numOwners, stats.sumOwned());

      double totalCapacity = 0;
      for (Float f : capacityFactors.values()) totalCapacity += f;
      double estimatedSegmentsPerCapacity = numSegments / totalCapacity;
      for (Address node : ch.getMembers()) {
         distribution[distIndex] = stats.getOwned(node) / capacityFactors.get(node) / numOwners / estimatedSegmentsPerCapacity;
         distributionPrimary[distIndex] = (stats.getPrimaryOwned(node) / capacityFactors.get(node) / estimatedSegmentsPerCapacity);
         distIndex++;
      }
      return distIndex;
   }

   private void addMetrics(Map<String, String> metrics, String prefix, int numSegments, int numOwners,
                           int numNodes, double[] distribution, double[] intervals) {
      double sum = 0;
      for (double x : distribution) sum += x;
      double mean = 1. * sum / numNodes / LOOPS;

      double variance = 0;
      for (double x : distribution) variance += (x - mean) * (x - mean);

      double stdDev = sqrt(variance);
      // metrics.put(prefix + " relative standard deviation", stdDev / mean);

      double min = distribution[0];
      double max = distribution[distribution.length - 1];
      addDoubleMetric(metrics, prefix + " min", min / mean);
      addDoubleMetric(metrics, prefix + " max", max / mean);

      double[] intervalConfidence = new double[intervals.length];
      int intervalIndex = 0;
      for (int i = 0; i < distribution.length; i++) {
         double x = distribution[i];
         if (x > intervals[intervalIndex] * mean) {
            intervalConfidence[intervalIndex] = (double) i / distribution.length;
            intervalIndex++;
            if (intervalIndex >= intervals.length)
               break;
         }
      }
      for (int i = intervalIndex; i < intervals.length; i++) {
         intervalConfidence[i] = 1.;
      }

      for (int i = 0; i < intervals.length; i++) {
         if (intervals[i] < 1) {
            addPercentageMetric(metrics, String.format("%s %% < %3.2f", prefix, intervals[i]), intervalConfidence[i]);
         } else {
            addPercentageMetric(metrics, String.format("%s %% > %3.2f", prefix, intervals[i]), 1 - intervalConfidence[i]);
         }
      }

      double[] percentiles = new double[PERCENTILES.length];
      for (int i = 0; i < PERCENTILES.length; i++) {
         percentiles[i] = (double)distribution[(int) Math.ceil(PERCENTILES[i] * (LOOPS * numNodes)) - 1] / mean;
      }
      for (int i = 0; i < PERCENTILES.length; i++) {
         addDoubleMetric(metrics, String.format("%s %5.2f%% percentile", prefix, PERCENTILES[i] * 100), percentiles[i]);
      }
   }

   private void addDoubleMetric(Map<String, String> metrics, String name, double value) {
      metrics.put(name, String.format("%7.3f", value));
   }

   private void addPercentageMetric(Map<String, String> metrics, String name, double value) {
      metrics.put(name, String.format("%6.2f%%", value * 100));
   }
}
