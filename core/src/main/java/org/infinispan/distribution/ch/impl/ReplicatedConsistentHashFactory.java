package org.infinispan.distribution.ch.impl;

import org.infinispan.commons.hash.Hash;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.distribution.ch.ConsistentHashFactory;
import org.infinispan.marshall.core.Ids;
import org.infinispan.remoting.transport.Address;

import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Factory for ReplicatedConsistentHash.
 *
 * @author Dan Berindei
 * @author anistor@redhat.com
 * @since 5.2
 */
public class ReplicatedConsistentHashFactory implements ConsistentHashFactory<ReplicatedConsistentHash> {
   SyncConsistentHashFactory syncConsistentHashFactory = new SyncConsistentHashFactory();

   @Override
   public ReplicatedConsistentHash create(Hash hashFunction, int numOwners, int numSegments, List<Address> members,
                                          Map<Address, Float> capacityFactors) {
      // TODO Some tests still expect the coordinator to be the primary owner
      if (numSegments == 1) {
         return new ReplicatedConsistentHash(hashFunction, members, new int[]{0});
      }
      DefaultConsistentHash syncCH = syncConsistentHashFactory.create(hashFunction, 1, numSegments, members, null);
      int[] primaryOwners = getPrimaryOwnerIndexes(syncCH);
      return new ReplicatedConsistentHash(hashFunction, members, primaryOwners);
   }

   protected int[] getPrimaryOwnerIndexes(DefaultConsistentHash syncCH) {
      int numSegments = syncCH.getNumSegments();
      List<Address> members = syncCH.getMembers();
      int[] primaryOwners = new int[numSegments];
      for (int i = 0; i < numSegments; i++) {
         primaryOwners[i] = members.indexOf(syncCH.locatePrimaryOwnerForSegment(i));
      }
      return primaryOwners;
   }

   @Override
   public ReplicatedConsistentHash updateMembers(ReplicatedConsistentHash baseCH, List<Address> newMembers,
                                                 Map<Address, Float> actualCapacityFactors) {
      if (newMembers.equals(baseCH.getMembers()))
         return baseCH;

      if (baseCH.getNumSegments() == 1) {
         return new ReplicatedConsistentHash(baseCH.getHashFunction(), newMembers, new int[]{0});
      }

      // recompute primary ownership based on the new list of members (removes leavers)
      int numSegments = baseCH.getNumSegments();
      int[] primaryOwners = new int[numSegments];
      boolean foundOrphanSegments = false;
      for (int segmentId = 0; segmentId < numSegments; segmentId++) {
         Address primaryOwner = baseCH.locatePrimaryOwnerForSegment(segmentId);
         int primaryOwnerIndex = newMembers.indexOf(primaryOwner);
         primaryOwners[segmentId] = primaryOwnerIndex;
         if (primaryOwnerIndex == -1) {
            foundOrphanSegments = true;
         }
      }

      // ensure leavers are replaced with existing members so no segments are orphan
      if (foundOrphanSegments) {
         DefaultConsistentHash syncCH = syncConsistentHashFactory.create(baseCH.getHashFunction(), 1, numSegments,
               newMembers, null);
         for (int segmentId = 0; segmentId < primaryOwners.length; ++segmentId) {
            if (primaryOwners[segmentId] == -1) {
               Address primaryOwner = syncCH.locatePrimaryOwnerForSegment(segmentId);
               primaryOwners[segmentId] = newMembers.indexOf(primaryOwner);
            }
         }
      }
      return new ReplicatedConsistentHash(baseCH.getHashFunction(), newMembers, primaryOwners);
   }

   @Override
   public ReplicatedConsistentHash rebalance(ReplicatedConsistentHash baseCH) {
      return create(baseCH.getHashFunction(), baseCH.getNumOwners(), baseCH.getNumSegments(), baseCH.getMembers(), null);
   }

   @Override
   public ReplicatedConsistentHash union(ReplicatedConsistentHash ch1, ReplicatedConsistentHash ch2) {
      if (!ch1.getHashFunction().equals(ch2.getHashFunction())) {
         throw new IllegalArgumentException("The consistent hash objects must have the same hash function");
      }
      if (ch1.getNumSegments() != ch2.getNumSegments()) {
         throw new IllegalArgumentException("The consistent hash objects must have the same number of segments");
      }

      List<Address> unionMembers = new ArrayList<Address>(ch1.getMembers());
      for (Address member : ch2.getMembers()) {
         if (!unionMembers.contains(member)) {
            unionMembers.add(member);
         }
      }

      int[] primaryOwners = new int[ch1.getNumSegments()];
      for (int segmentId = 0; segmentId < primaryOwners.length; segmentId++) {
         Address primaryOwner = ch1.locatePrimaryOwnerForSegment(segmentId);
         int primaryOwnerIndex = unionMembers.indexOf(primaryOwner);
         primaryOwners[segmentId] = primaryOwnerIndex;
      }

      return new ReplicatedConsistentHash(ch1.getHashFunction(), unionMembers, primaryOwners);
   }

   @Override
   public boolean equals(Object other) {
      return other != null && other.getClass() == getClass();
   }

   @Override
   public int hashCode() {
      return -6053;
   }

   public static class Externalizer extends AbstractExternalizer<ReplicatedConsistentHashFactory> {

      @Override
      public void writeObject(ObjectOutput output, ReplicatedConsistentHashFactory chf) {
      }

      @Override
      @SuppressWarnings("unchecked")
      public ReplicatedConsistentHashFactory readObject(ObjectInput unmarshaller) {
         return new ReplicatedConsistentHashFactory();
      }

      @Override
      public Integer getId() {
         return Ids.REPLICATED_CONSISTENT_HASH_FACTORY;
      }

      @Override
      public Set<Class<? extends ReplicatedConsistentHashFactory>> getTypeClasses() {
         return Collections.<Class<? extends ReplicatedConsistentHashFactory>>singleton(ReplicatedConsistentHashFactory.class);
      }
   }
}
