package org.infinispan.distribution.ch.impl;

import org.infinispan.commons.hash.Hash;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.distribution.ch.ConsistentHashFactory;
import org.infinispan.marshall.core.Ids;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * One of the assumptions people made on consistent hashing involves thinking
 * that given a particular key and same topology, it would produce the same
 * consistent hash value no matter which cache it was stored in. However,
 * that's not exactly the case in Infinispan.
 *
 * In order to the optimise the number of segments moved on join/leave,
 * Infinispan uses a consistent hash that depends on the previous consistent
 * hash. Given two caches, even if they have exactly the same members, it's
 * very easy for the consistent hash history to differ, e.g. if 2 nodes join
 * you might see two separate topology change in one cache and a single
 * topology change in the other. The reason for that each node has to send a
 * {@link org.infinispan.topology.CacheTopologyControlCommand} for each cache
 * it wants to join and Infinispan can and does batch cache topology changes.
 * For example, if a rebalance is in progress, joins are queued and send in
 * one go when the rebalance has finished.
 *
 * This {@link org.infinispan.distribution.ch.ConsistentHashFactory} implementation avoids any of the issues
 * mentioned and guarantees that multiple caches with the same members will
 * have the same consistent hash.
 *
 * It has a drawback compared to {@link DefaultConsistentHashFactory} though:
 * it can potentially move a lot more segments during a rebalance than
 * strictly necessary because it's not taking advantage of the optimisation
 * mentioned above.
 *
 * @author Dan Berindei
 * @since 5.2
 */
public class SyncConsistentHashFactory implements ConsistentHashFactory<DefaultConsistentHash> {

   public static final float OWNED_SEGMENTS_ALLOWED_VARIATION = 0.10f;
   public static final float PRIMARY_OWNED_SEGMENTS_ALLOWED_VARIATION = 0.20f;

   private static final Log log = LogFactory.getLog(SyncConsistentHashFactory.class);

   @Override
   public DefaultConsistentHash create(Hash hashFunction, int numOwners, int numSegments, List<Address> members,
                                       Map<Address, Float> capacityFactors) {
      checkCapacityFactors(members, capacityFactors);

      Builder builder = createBuilder(hashFunction, numOwners, numSegments, members, capacityFactors);
      builder.populateVirtualNodes(numSegments);
      builder.copyOwners();

      return new DefaultConsistentHash(hashFunction, numOwners, numSegments, members, capacityFactors, builder.segmentOwners);
   }

   protected Builder createBuilder(Hash hashFunction, int numOwners, int numSegments, List<Address> members, Map<Address, Float> capacityFactors) {
      return new Builder(hashFunction, numOwners, numSegments, members, capacityFactors);
   }

   protected void checkCapacityFactors(List<Address> members, Map<Address, Float> capacityFactors) {
      if (capacityFactors != null) {
         float totalCapacity = 0;
         for (Address node : members) {
            Float capacityFactor = capacityFactors.get(node);
            if (capacityFactor == null || capacityFactor < 0)
               throw new IllegalArgumentException("Invalid capacity factor for node " + node);
            totalCapacity += capacityFactor;
         }
         if (totalCapacity == 0)
            throw new IllegalArgumentException("There must be at least one node with a non-zero capacity factor");
      }
   }

   @Override
   public DefaultConsistentHash updateMembers(DefaultConsistentHash baseCH, List<Address> newMembers,
                                              Map<Address, Float> actualCapacityFactors) {
      checkCapacityFactors(newMembers, actualCapacityFactors);

      // The ConsistentHashFactory contract says we should return the same instance if we're not making changes
      boolean sameCapacityFactors = actualCapacityFactors == null ? baseCH.getCapacityFactors() == null :
            actualCapacityFactors.equals(baseCH.getCapacityFactors());
      if (newMembers.equals(baseCH.getMembers()) && sameCapacityFactors)
         return baseCH;

      int numSegments = baseCH.getNumSegments();
      int numOwners = baseCH.getNumOwners();

      // We assume leavers are far fewer than members, so it makes sense to check for leavers
      HashSet<Address> leavers = new HashSet<Address>(baseCH.getMembers());
      leavers.removeAll(newMembers);

      // Create a new "balanced" CH in case we need to allocate new owners for segments with 0 owners
      DefaultConsistentHash rebalancedCH = null;

      // Remove leavers
      List<Address>[] newSegmentOwners = new List[numSegments];
      for (int i = 0; i < numSegments; i++) {
         List<Address> owners = new ArrayList<Address>(baseCH.locateOwnersForSegment(i));
         owners.removeAll(leavers);
         if (!owners.isEmpty()) {
            newSegmentOwners[i] = owners;
         } else {
            // this segment has 0 owners, fix it
            if (rebalancedCH == null) {
               rebalancedCH = create(baseCH.getHashFunction(), numOwners, numSegments, newMembers, actualCapacityFactors);
            }
            newSegmentOwners[i] = rebalancedCH.locateOwnersForSegment(i);
         }
      }

      return new DefaultConsistentHash(baseCH.getHashFunction(), numOwners, numSegments, newMembers,
            actualCapacityFactors, newSegmentOwners);
   }

   @Override
   public DefaultConsistentHash rebalance(DefaultConsistentHash baseCH) {
      DefaultConsistentHash rebalancedCH = create(baseCH.getHashFunction(), baseCH.getNumOwners(),
            baseCH.getNumSegments(), baseCH.getMembers(), baseCH.getCapacityFactors());

      // the ConsistentHashFactory contract says we should return the same instance if we're not making changes
      if (rebalancedCH.equals(baseCH))
         return baseCH;

      return rebalancedCH;
   }

   @Override
   public DefaultConsistentHash union(DefaultConsistentHash ch1, DefaultConsistentHash ch2) {
      return ch1.union(ch2);
   }

   @Override
   public boolean equals(Object other) {
      return other != null && other.getClass() == getClass();
   }

   @Override
   public int hashCode() {
      return -10009;
   }

   protected static class Builder {
      protected final Hash hashFunction;
      protected final int numOwners;
      protected final Map<Address, Float> capacityFactors;
      protected final int actualNumOwners;
      protected final int numSegments;
      protected final List<Address> sortedMembers;
      protected final int segmentSize;
      protected final float totalCapacity;

      protected final List<Address>[] segmentOwners;
      private Map<Address, List<VirtualNode>> virtualNodeHashes;
      protected final OwnershipStatistics stats;

      protected Builder(Hash hashFunction, int numOwners, int numSegments, List<Address> members,
                      Map<Address, Float> capacityFactors) {
         this.hashFunction = hashFunction;
         this.numSegments = numSegments;
         this.numOwners = numOwners;
         this.actualNumOwners = computeActualNumOwners(numOwners, members, capacityFactors);
         this.sortedMembers = sort(members, capacityFactors);
         this.capacityFactors = populateCapacityFactors(capacityFactors, sortedMembers);
         this.segmentSize = Util.getSegmentSize(numSegments);
         this.totalCapacity = computeTotalCapacity();

         this.virtualNodeHashes = populateVirtualNodes(numSegments);
         this.segmentOwners = new List[numSegments];
         for (int i = 0; i < numSegments; i++) {
            segmentOwners[i] = new ArrayList<Address>(actualNumOwners);
         }
         stats = new OwnershipStatistics(members);
      }

      private Map<Address, Float> populateCapacityFactors(Map<Address, Float> capacityFactors, List<Address> sortedMembers) {
         if (capacityFactors != null)
            return capacityFactors;

         Map<Address, Float> realCapacityFactors = new HashMap<>();
         for (Address member : sortedMembers) {
            realCapacityFactors.put(member, 1.0f);
         }
         return realCapacityFactors;
      }

      protected void addOwnerNoCheck(int segment, Address owner, boolean updateStats) {
         segmentOwners[segment].add(owner);
         if (updateStats) {
            stats.incOwned(owner);
            if (segmentOwners[segment].size() == 1) {
               stats.incPrimaryOwned(owner);
            }
         }
      }

      protected float computeTotalCapacity() {
         if (capacityFactors == null)
            return sortedMembers.size();

         float totalCapacity = 0;
         for (Address member : sortedMembers) {
            totalCapacity += capacityFactors.get(member);
         }
         return totalCapacity;
      }

      public int computeActualNumOwners(int numOwners, List<Address> members, Map<Address, Float> capacityFactors) {
         int nodesWithLoad = members.size();
         if (capacityFactors != null) {
            nodesWithLoad = 0;
            for (Address node : members) {
               if (capacityFactors.get(node) != 0) {
                  nodesWithLoad++;
               }
            }
         }
         return Math.min(numOwners, nodesWithLoad);
      }

      protected List<Address> sort(List<Address> members, final Map<Address, Float> capacityFactors) {
         ArrayList<Address> result = new ArrayList<Address>(members);
         Collections.sort(result, new Comparator<Address>() {
            @Override
            public int compare(Address o1, Address o2) {
               // Sort descending by capacity factor and ascending by address (UUID)
               int capacityComparison = capacityFactors != null ? capacityFactors.get(o1).compareTo(capacityFactors
                     .get(o2)) : 0;
               return capacityComparison != 0 ? -capacityComparison : o1.compareTo(o2);
            }
         });
         return result;
      }

      protected void copyOwners() {
         for (int segment = 0; segment < numSegments; segment++) {
            // pick one virtual node for each member, the one closest to the segment start
            final int segmentHash = segment * segmentSize;
            List<VirtualNode> closestNodes = new ArrayList<>(sortedMembers.size());
            for (List<VirtualNode> memberVirtualNodes : virtualNodeHashes.values()) {
               VirtualNode closestVirtualNode = findClosestVirtualNode(memberVirtualNodes, segmentHash);
               closestNodes.add(closestVirtualNode);
            }

            addOwnersForSegment(actualNumOwners, segment, closestNodes);

            // Update the stats after we're done with the priority queue (and its comparator)
            List<Address> owners = segmentOwners[segment];
            stats.incPrimaryOwned(owners.get(0));
            for (int i = 0; i < owners.size(); i++) {
               stats.incOwned(owners.get(i));
            }
         }
      }

      protected void addOwnersForSegment(final int numCopies, int segment, List<VirtualNode> closestVirtualNodes) {
         List<Address> owners = segmentOwners[segment];

         // sort virtual nodes by distance from segment start
         final int segmentHash = segment * segmentSize;
         Comparator<VirtualNode> distanceComparator = new Comparator<VirtualNode>() {
            @Override
            public int compare(VirtualNode node1, VirtualNode node2) {
               // Compare on hash distance only
               return Integer.compare(circleDistance(node1.hash, segmentHash), circleDistance(node2.hash, segmentHash));
            }
         };
         PriorityQueue<VirtualNode> candidates = new PriorityQueue<>(sortedMembers.size(), distanceComparator);
         candidates.addAll(closestVirtualNodes);

         PriorityQueue<VirtualNode> candidatesCopy;
         // Add closest node as primary owner, ignore members with too many primary-owned segments
         candidatesCopy = new PriorityQueue<>(candidates);
         while (owners.size() < 1 && !candidatesCopy.isEmpty()) {
            VirtualNode virtualNode = candidatesCopy.poll();
            Address address = virtualNode.address;
            double expectedPrimarySegments = computeExpectedSegmentsForNode(address, 1);
            int maxPrimary = (int) (expectedPrimarySegments + Math.max(expectedPrimarySegments * PRIMARY_OWNED_SEGMENTS_ALLOWED_VARIATION, 1));
            if (stats.getPrimaryOwned(address) < maxPrimary) {
               double expectedSegments = computeExpectedSegmentsForNode(address, numCopies);
               int maxSegments = (int) (expectedSegments + Math.max(expectedSegments * OWNED_SEGMENTS_ALLOWED_VARIATION, 1));
               if (stats.getOwned(address) < maxSegments) {
                  addOwner(segment, address, false);
               }
            }
         }

         addBackupOwnersForSegment(numCopies, segment, candidates, owners);
      }

      protected void addBackupOwnersForSegment(int numCopies, int segment, PriorityQueue<VirtualNode> candidates, List<Address> owners) {
         PriorityQueue<VirtualNode> candidatesCopy;
         // Add backup owner, ignore members with too many backup-owned segments
         // or too many primary-owned segments
         candidatesCopy = new PriorityQueue<>(candidates);
         while (owners.size() < numCopies && !candidatesCopy.isEmpty()) {
            VirtualNode virtualNode = candidatesCopy.poll();
            Address address = virtualNode.address;
            double expectedSegments = computeExpectedSegmentsForNode(address, numCopies);
            int maxSegments = (int) (expectedSegments + Math.max(expectedSegments * OWNED_SEGMENTS_ALLOWED_VARIATION, 1));
            if (stats.getOwned(address) < maxSegments) {
               addOwner(segment, address, false);
            }
         }
         // Last-ditch attempt, in case neither of the previous attempts succeeded to fill the owners list
         if (owners.size() < numCopies) {
            candidatesCopy = new PriorityQueue<>(candidates);
            while (owners.size() < numCopies && !candidatesCopy.isEmpty()) {
               VirtualNode virtualNode = candidatesCopy.poll();
               Address address = virtualNode.address;
               addOwner(segment, address, false);
            }
         }
      }

      private VirtualNode findClosestVirtualNode(List<VirtualNode> virtualNodes, int segmentHash) {
         int i = Collections.binarySearch(virtualNodes, new VirtualNode(segmentHash, null));
         if (i >= 0) {
            return virtualNodes.get(i);
         }
         // i = (-(insertion point) - 1)
         int insertionPoint = -i - 1;
         int size = virtualNodes.size();
         int nodeBefore = insertionPoint > 0 ? insertionPoint - 1 : size - 1;
         int nodeAfter = insertionPoint < size ? insertionPoint : 0;
         int distanceBefore = circleDistance(segmentHash, virtualNodes.get(nodeBefore).hash);
         int distanceAfter = circleDistance(segmentHash, virtualNodes.get(nodeAfter).hash);
         return distanceBefore < distanceAfter ? virtualNodes.get(nodeBefore) : virtualNodes.get(nodeAfter);
      }

      protected Map<Address, List<VirtualNode>> populateVirtualNodes(int numSegments) {
         int numVirtualNodes = Math.min(numSegments, 100);
         Map<Address, List<VirtualNode>> virtualNodes = new HashMap<>(sortedMembers.size());
         for (Address member : sortedMembers) {
            if (capacityFactors.get(member) == 0)
               continue;

            List<VirtualNode> list = new ArrayList<>(numVirtualNodes * sortedMembers.size());
            int nodeHash = normalizedHash(hashFunction, member.hashCode());
            list.add(new VirtualNode(nodeHash, member));

            for (int virtualNode = 1; virtualNode < numVirtualNodes; virtualNode++) {
               // Add the virtual node count after applying MurmurHash on the node's hashCode
               // to make up for badly spread test addresses.
               int virtualNodeHash = normalizedHash(hashFunction, nodeHash + virtualNode);
               list.add(new VirtualNode(virtualNodeHash, member));
            }
            Collections.sort(list);
            virtualNodes.put(member, list);
         }
         return virtualNodes;
      }

      protected double computeExpectedSegmentsForNode(Address node, int numCopies) {
         Float nodeCapacityFactor = capacityFactors.get(node);
         if (nodeCapacityFactor == 0)
            return 0;

         double remainingCapacity = totalCapacity;
         double remainingCopies = numCopies * numSegments;
         for (Address a : sortedMembers) {
            float capacityFactor = capacityFactors.get(a);
            double nodeSegments = capacityFactor / remainingCapacity * remainingCopies;
            if (nodeSegments > numSegments) {
               nodeSegments = numSegments;
               remainingCapacity -= capacityFactor;
               remainingCopies -= nodeSegments;
               if (node.equals(a))
                  return nodeSegments;
            } else {
               // All the nodes from now on will have less than numSegments segments, so we can stop the iteration
               if (!node.equals(a)) {
                  nodeSegments = nodeCapacityFactor / remainingCapacity * remainingCopies;
               }
               return nodeSegments;
            }
         }
         throw new IllegalStateException("The nodes collection does not include " + node);
      }

      protected boolean addOwner(int segment, Address candidate, boolean updateStats) {
         List<Address> owners = segmentOwners[segment];
         if (owners.contains(candidate)) {
            return false;
         }

         addOwnerNoCheck(segment, candidate, updateStats);
         return true;
      }

      protected int normalizedHash(Hash hashFunction, int hashcode) {
         return hashFunction.hash(hashcode) & Integer.MAX_VALUE;
      }

      public int circleDistance(int hash1, int hash2) {
         int distance = Math.abs(hash1 - hash2);
         return (distance & 0x4000000) == 0 ? distance : Integer.MAX_VALUE - distance;
      }
   }

   public static class VirtualNode implements Comparable<VirtualNode> {
      public final int hash;
      public final Address address;

      public VirtualNode(int hash, Address address) {
         this.hash = hash;
         this.address = address;
      }

      @Override
      public int compareTo(VirtualNode o) {
         int hashDiff = Integer.compare(hash, o.hash);
         if (hashDiff != 0) {
            return hashDiff;
         } else {
            // A null address is always smaller than a non-null one
            if (address != null && o.address != null) {
               return address.compareTo(o.address);
            } else if (address == null) {
               return o.address == null ? 0 : -1;
            } else {
               return 1;
            }
         }
      }

      @Override
      public boolean equals(Object o) {
         if (this == o)
            return true;
         if (o == null || getClass() != o.getClass())
            return false;

         VirtualNode that = (VirtualNode) o;

         if (hash != that.hash)
            return false;
         if (address != null ? !address.equals(that.address) : that.address != null)
            return false;

         return true;
      }

      @Override
      public int hashCode() {
         int result = hash;
         result = 31 * result + (address != null ? address.hashCode() : 0);
         return result;
      }

      @Override
      public String toString() {
         return hash + ":" + address;
      }
   }

   public static class Externalizer extends AbstractExternalizer<SyncConsistentHashFactory> {

      @Override
      public void writeObject(ObjectOutput output, SyncConsistentHashFactory chf) {
      }

      @Override
      @SuppressWarnings("unchecked")
      public SyncConsistentHashFactory readObject(ObjectInput unmarshaller) {
         return new SyncConsistentHashFactory();
      }

      @Override
      public Integer getId() {
         return Ids.SYNC_CONSISTENT_HASH_FACTORY;
      }

      @Override
      public Set<Class<? extends SyncConsistentHashFactory>> getTypeClasses() {
         return Collections.<Class<? extends SyncConsistentHashFactory>>singleton(SyncConsistentHashFactory.class);
      }
   }
}
