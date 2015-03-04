package org.infinispan.distribution.ch;

import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.distribution.ch.impl.DefaultConsistentHash;
import org.infinispan.distribution.ch.impl.SyncConsistentHashFactory;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.AbstractInfinispanTest;
import org.jgroups.util.UUID;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Test the even distribution and number of moved segments after rebalance for {@link SyncConsistentHashFactory}
 *
 * @author Dan Berindei
 * @since 5.2
 */
@Test(groups = "unit", testName = "distribution.ch.SyncConsistentHashFactoryTest")
public class SyncConsistentHashFactoryTest extends AbstractInfinispanTest {

   // numbers of nodes to test
   public static final int[] NUM_NODES = {10, 50};
   // numbers of virtual nodes to test
   public static final int[] NUM_SEGMENTS = {1024, 2048};
   // number of key owners
   public static final int[] NUM_OWNERS = {2, 3};

   // controls precision + duration of test
   public static final int LOOPS = 200;

   private ConsistentHashFactory<DefaultConsistentHash> chf = new SyncConsistentHashFactory();
   private Random random = new Random();

   protected DefaultConsistentHash createConsistentHash(int numSegments, int numOwners, List<Address> members,
         Map<Address, Float> capacityFactors) {
      MurmurHash3 hash = MurmurHash3.getInstance();
      DefaultConsistentHash ch = chf.create(hash, numOwners, numSegments, members, capacityFactors);
      return ch;
   }

   private Map<Address, Float> createCapacityFactors(List<Address> members) {
      Map<Address, Float> capacityFactors = new HashMap<>(members.size());
      for (Address member : members) {
         capacityFactors.put(member, (float) (random.nextInt(2) + 1));
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

   public void testRedistribution() {
      int combinationIndex = 0;
      for (int nn : NUM_NODES) {
         for (int no : NUM_OWNERS) {
            for (int ns : NUM_SEGMENTS) {
               performRedistribution(combinationIndex, ns, no, nn);
               combinationIndex++;
            }
         }
      }
   }

   private void performRedistribution(int combinationIndex, int numSegments, int numOwners, int numNodes) {
      int movedSegmentCopies = 0;
      for (int i = 0; i < LOOPS; i++) {
         movedSegmentCopies += doLoop(combinationIndex, numSegments, numOwners, numNodes, i);
      }
      log.debugf("combination %d, ns = %d, no = %d, nn = %d, moved copies = %d", combinationIndex,
            numSegments, numOwners, numNodes, movedSegmentCopies);
      System.out.printf("combination %d, ns = %d, no = %d, nn = %d, moved copies/loop = %d\n", combinationIndex,
            numSegments, numOwners, numNodes, movedSegmentCopies / LOOPS);
   }

   private int doLoop(int combinationIndex, int numSegments, int numOwners, int numNodes, int loopIndex) {
      List<Address> members = createAddresses(numNodes);
//      log.tracef("Combination %d, loop %d, members %s", combinationIndex, loopIndex, members);
      Map<Address, Float> capacityFactors = createCapacityFactors(members);
      DefaultConsistentHash baseCH = createConsistentHash(numSegments, numOwners, members, capacityFactors);

      int totalMovedSegmentCopies = 0;
      List<Address> remainingMembers = new ArrayList<>(members);
      while (remainingMembers.size() > 1) {
         int memberToRemove = random.nextInt(remainingMembers.size());
         Address removedMember = remainingMembers.remove(memberToRemove);
         DefaultConsistentHash ch = createConsistentHash(numSegments, numOwners, remainingMembers, capacityFactors);

         int movedSegmentCopies = 0;
         for (int segment = 0; segment < numSegments; segment++) {
            List<Address> oldOwners = new ArrayList<>( baseCH.locateOwnersForSegment(segment));
            List<Address> newOwners = ch.locateOwnersForSegment(segment);
            oldOwners.removeAll(newOwners);
            oldOwners.remove(removedMember);
            if (!oldOwners.isEmpty()) {
               movedSegmentCopies += oldOwners.size();
            }
         }

         if (movedSegmentCopies > 0) {
//            log.tracef("Missing %d of the old owners in the new CH after removing %s (of %d)", movedSegmentCopies,
//                  removedMember, remainingMembers.size() + 1);
//               log.tracef(baseCH.getRoutingTableAsString());
//               log.tracef(ch.getRoutingTableAsString());
         }
         baseCH = ch;
         totalMovedSegmentCopies += movedSegmentCopies;
      }

      return totalMovedSegmentCopies;
   }

}
