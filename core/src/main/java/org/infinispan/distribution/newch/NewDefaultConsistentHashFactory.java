/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.distribution.newch;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.infinispan.commons.hash.Hash;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Creates a new instance of {@link NewDefaultConsistentHash}.
 *
 * @author Dan Berindei
 * @since 5.2
 */
public class NewDefaultConsistentHashFactory implements NewConsistentHashFactory {

   private static final Log log = LogFactory.getLog(NewDefaultConsistentHashFactory.class);

   @Override
   public boolean needNewConsistentHash(NewConsistentHash existingCH, List<Address> newMembers) {
      NewDefaultConsistentHash baseDCH = (NewDefaultConsistentHash) existingCH;

      if (!newMembers.containsAll(baseDCH.getNodes()))
         return true;

      int numOwners = baseDCH.getNumOwners();
      int numSegments = baseDCH.getNumSegments();
      for (int i = 0; i < numSegments; i++) {
         if (baseDCH.locateOwnersForSegment(i).size() != numOwners)
            return true;
      }

      int maxPrimaryOwnedSegments = (int) Math.ceil((double) numSegments / newMembers.size());
      int maxOwnedSegments = (int) Math.ceil((double) numSegments / newMembers.size() * numOwners);
      NewDefaultConsistentHashFactory.CHStats stats = computeStatistics(baseDCH, newMembers);
      for (Address a : newMembers) {
         if (stats.getPrimaryOwned(a) > maxPrimaryOwnedSegments || stats.getOwned(a) > maxOwnedSegments)
            return true;
      }

      return false;
   }

   @Override
   public NewConsistentHash createConsistentHash(Hash hashFunction, int numOwners, int numSegments, List<Address> members) {
      int size = members.size();
      List<Address>[] ownerLists = new List[numSegments];
      for (int i = 0; i < numSegments; i++) {
         List<Address> owners = new ArrayList<Address>(numOwners);
         for (int j = 0; j < numOwners; j++) {
            owners.add(members.get((i + j) % size));
         }
         ownerLists[i] = owners;
      }
      return new NewDefaultConsistentHash(hashFunction, numSegments, numOwners, members, ownerLists);
   }

   @Override
   public NewConsistentHash createConsistentHash(NewConsistentHash baseCH, List<Address> newMembers) {
      NewDefaultConsistentHash baseDCH = (NewDefaultConsistentHash) baseCH;

      // TODO Add a separate method to deal with leavers, assume createConsistentHash never has to deal with leavers
      // 1. remove leavers
      if (!newMembers.containsAll(baseCH.getNodes())) {
         return removeLeavers(baseCH, newMembers, baseDCH);
      }

      // 2. add new owners where necessary
      NewDefaultConsistentHash phase2CH = addNewOwners(baseDCH, newMembers);
      if (phase2CH != null)
         return phase2CH;


      // 3. choose new primary owners and remove extra owners
      NewDefaultConsistentHash phase3CH = pruneOldOwners(baseDCH);
      return phase3CH;
   }

   private NewDefaultConsistentHash addNewOwners(NewDefaultConsistentHash baseDCH, List<Address> nodes) {
      // Assume nodes.containsAll(baseDCH.getNodes())
      // The goal of this phase is to assign new owners to the segments so that
      // * num_owners(s) == numOwners, for each segment s
      // * floor(numSegments/numNodes) <= num_segments_primary_owned(n) for each node n
      // * floor(numSegments*numOwners/numNodes) <= num_segments_owned(n) for each node n
      // It will not change primary owners or remove old owners, but it will prepare things for the next phase
      // to remove owners so that
      // * num_segments_primary_owned(n) <= ceil(numSegments/numNodes) for each node n
      // * num_segments_owned(n) <= ceil(numSegments*numOwners/numNodes) for each node n

      Hash hashFunction = baseDCH.getHashFunction();
      int numOwners = baseDCH.getNumOwners();
      int numSegments = baseDCH.getNumSegments();

      boolean madeChanges = false;
      CHStats stats = computeStatistics(baseDCH, nodes);

      // Copy the owners list out of the old CH
      List<Address>[] ownerLists = extractOwnerLists(baseDCH);

      madeChanges |= addPrimaryOwners(nodes, ownerLists, numOwners, numSegments, stats);

      madeChanges |= addBackupOwners(nodes, ownerLists, numOwners, numSegments, stats);

      if (!madeChanges)
         return null;

      return new NewDefaultConsistentHash(hashFunction, numSegments, numOwners, nodes, ownerLists);
   }

   private List<Address>[] extractOwnerLists(NewDefaultConsistentHash baseDCH) {
      int numSegments = baseDCH.getNumSegments();
      List<Address>[] ownerLists = new List[numSegments];
      for (int i = 0; i < numSegments; i++) {
         ownerLists[i] = new ArrayList<Address>(baseDCH.locateOwnersForSegment(i));
      }
      return ownerLists;
   }

   private boolean addPrimaryOwners(List<Address> nodes, List<Address>[] ownerLists, int numOwners,
                                    int numSegments, CHStats stats) {
      int numNodes = nodes.size();

      // Compute how many segments each node has to primary-own.
      // If numSegments is not divisible by numNodes, older nodes will own 1 extra segment.
      Map<Address, Integer> expectedPrimaryOwned = new HashMap<Address, Integer>(numNodes);
      for (int i = 0; i < numNodes; i++) {
         if (i < numSegments % numNodes) {
            expectedPrimaryOwned.put(nodes.get(i), numSegments / numNodes + 1);
         } else {
            expectedPrimaryOwned.put(nodes.get(i), numSegments / numOwners);
         }
      }

      // Find the nodes that need to primary-own more segments
      // A node can appear multiple times in this list, once for each new segment
      List<Address> newPrimaryOwners = new LinkedList<Address>();
      for (Address node : nodes) {
         int primaryOwned = stats.getPrimaryOwned(node);
         for (int i = 0; i < expectedPrimaryOwned.get(node) - primaryOwned; i++) {
            newPrimaryOwners.add(node);
         }
      }
      if (newPrimaryOwners.isEmpty())
         return false;

      // Iterate over the segments, change ownership if the primary owner has > expectedPrimaryOwned
      // Iterate backwards in an attempt to make the algorithm more stable
      for (int i = numSegments - 1; i >= 0; i--) {
         List<Address> owners = ownerLists[i];
         Address primaryOwner = owners.get(0);
         int primaryOwned = stats.getPrimaryOwned(primaryOwner);
         if (primaryOwned > expectedPrimaryOwned.get(primaryOwner)) {
            // Need to pass primary ownership of this segment to another node.
            // But that node might already be a backup owner.
            // Note: In the next phase, the new primary owner will *have* to be one of the backup owners.
            Address newPrimaryOwner = removeOneOf(newPrimaryOwners, owners);
            // If the existing backup owners can't primary-own new segments, add a new backup owner
            if (newPrimaryOwner == null) {
               newPrimaryOwner = newPrimaryOwners.remove(0);
               stats.incOwned(newPrimaryOwner);
               assert stats.getOwned(newPrimaryOwner) <= numSegments * numOwners / numNodes
                     : "Can't add a node as primary owner since it already has too many owned segments";
            }
            owners.add(newPrimaryOwner);
            stats.decPrimaryOwned(primaryOwner);
            stats.incPrimaryOwned(newPrimaryOwner);
         }
      }
      return true;
   }

   private boolean addBackupOwners(List<Address> nodes, List<Address>[] ownerLists, int numOwners,
                                   int numSegments, CHStats stats) {
      int numNodes = nodes.size();

      // Compute how many segments each node has to own.
      // If numSegments*numOwners is not divisible by numNodes, older nodes will own 1 extra segment.
      Map<Address, Integer> expectedOwned = new HashMap<Address, Integer>(numNodes);
      for (int i = 0; i < numNodes; i++) {
         if (numNodes - i > (numSegments * numOwners) % numNodes) {
            expectedOwned.put(nodes.get(i), numSegments * numOwners / numNodes + 1);
         } else {
            expectedOwned.put(nodes.get(i), numSegments * numOwners / numNodes);
         }
      }

      // Find the nodes that need to own more segments
      // A node can appear multiple times in this list, once for each new segment
      List<Address> newOwners = new LinkedList<Address>();
      for (Address node : nodes) {
         int owned = stats.getOwned(node);
         for (int i = 0; i < expectedOwned.get(node) - owned; i++) {
            newOwners.add(node);
         }
      }
      if (newOwners.isEmpty())
         return false;

      // Iterate over the segments, change ownership if an owner has > expectedOwned
      // and make a note of how many other owners we need for each segment
      // We need to change existing owners first, otherwise for some segments
      // all the potential new owners left could be already owners of those segments.
      int[] neededNewOwners = new int[numSegments];
      for (int i = numSegments - 1; i >= 0; i--) {
         List<Address> owners = ownerLists[i];
         neededNewOwners[i] = numOwners - owners.size();
         for (int j = owners.size() - 1; j >= 1; j++) {
            Address owner = owners.get(j);
            int owned = stats.getOwned(owner);
            if (owned > expectedOwned.get(owner)) {
               // Need to pass ownership of this segment to another node.
               Address newOwner = removeNotOneOf(newOwners, owners);
               assert newOwner != null : "As long as there is a node with too many owned segments, there" +
                     "should also be a node with too few owned segments";
               owners.add(newOwner);
            }
         }
      }

      // Now add any required new owners
      for (int i = 0; i < numSegments; i++) {
         List<Address> owners = ownerLists[i];
         for (int j = 0; j < neededNewOwners[i]; j++) {
            Address newOwner = removeNotOneOf(newOwners, owners);
            assert newOwner != null : "As long as there is a segment with not enough owners, there" +
                  "should also be a node with too few owned segments";
            owners.add(newOwner);
         }
      }

      assert newOwners.isEmpty() : "Can't still have nodes to assign if all the segments have enough owners";
      return true;
   }

   private NewDefaultConsistentHash pruneOldOwners(NewDefaultConsistentHash baseDCH) {
      // Assume
      // * num_owners(s) == numOwners, for each segment s
      // * floor(numSegments/numNodes) <= num_segments_primary_owned(n) for each node n
      // * floor(numSegments*numOwners/numNodes) <= num_segments_owned(n) for each node n
      // Remove owners so that
      // * num_segments_primary_owned(n) <= ceil(numSegments/numNodes) for each node n
      // * num_segments_owned(n) <= ceil(numSegments*numOwners/numNodes) for each node n

      Hash hashFunction = baseDCH.getHashFunction();
      int numOwners = baseDCH.getNumOwners();
      int numSegments = baseDCH.getNumSegments();
      List<Address> nodes = baseDCH.getNodes();

      CHStats stats = computeStatistics(baseDCH, nodes);

      // Copy the owners list out of the old CH
      List<Address>[] ownerLists = extractOwnerLists(baseDCH);

      prunePrimaryOwners(nodes, ownerLists, numOwners, numSegments, stats);

      pruneBackupOwners(nodes, ownerLists, numOwners, numSegments, stats);

      return new NewDefaultConsistentHash(hashFunction, numSegments, numOwners, nodes, ownerLists);
   }

   private void prunePrimaryOwners(List<Address> nodes, List<Address>[] ownerLists, int numOwners,
                                    int numSegments, CHStats stats) {
      int numNodes = nodes.size();

      // Compute how many segments each node has to primary-own.
      // If numSegments is not divisible by numNodes, older nodes will own 1 extra segment.
      Map<Address, Integer> expectedPrimaryOwned = new HashMap<Address, Integer>(numNodes);
      for (int i = 0; i < numNodes; i++) {
         if (i < numSegments % numNodes) {
            expectedPrimaryOwned.put(nodes.get(i), numSegments / numNodes + 1);
         } else {
            expectedPrimaryOwned.put(nodes.get(i), numSegments / numOwners);
         }
      }

      // Find the nodes that need to primary-own more segments
      // A node can appear multiple times in this list, once for each new segment
      List<Address> newPrimaryOwners = new LinkedList<Address>();
      for (Address node : nodes) {
         int primaryOwned = stats.getPrimaryOwned(node);
         for (int i = 0; i < expectedPrimaryOwned.get(node) - primaryOwned; i++) {
            newPrimaryOwners.add(node);
         }
      }

      // Iterate over the segments, change ownership if the primary owner has > expectedPrimaryOwned
      // TODO We iterate backwards in an attempt to make the algorithm more stable - we should rebuild the stats incrementally instead
      for (int i = numSegments - 1; i >= 0; i--) {
         List<Address> owners = ownerLists[i];
         Address primaryOwner = owners.get(0);
         int primaryOwned = stats.getPrimaryOwned(primaryOwner);
         if (primaryOwned > expectedPrimaryOwned.get(primaryOwner)) {
            // Need to pass primary ownership of this segment to one of the backup owners.
            Address newPrimaryOwner = removeOneOf(newPrimaryOwners, owners);
            assert newPrimaryOwner != null : "Could not find a suitable replacement primary owner";
            assert stats.getOwned(newPrimaryOwner) <= numSegments * numOwners / numNodes
                  : "Can't add a node as primary owner since it already has too many owned segments";
            owners.remove(newPrimaryOwner);
            owners.add(0, newPrimaryOwner);
            stats.decPrimaryOwned(primaryOwner);
            stats.incPrimaryOwned(newPrimaryOwner);
         }
      }

      assert newPrimaryOwners.isEmpty() : "Can't still have nodes to ";
   }

   private void pruneBackupOwners(List<Address> nodes, List<Address>[] ownerLists, int numOwners,
                                  int numSegments, CHStats stats) {
      int numNodes = nodes.size();

      // Compute how many segments each node has to own.
      // If numSegments*numOwners is not divisible by numNodes, older nodes will own 1 extra segment.
      Map<Address, Integer> expectedOwned = new HashMap<Address, Integer>(numNodes);
      for (int i = 0; i < numNodes; i++) {
         if (numNodes - i > (numSegments * numOwners) % numNodes) {
            expectedOwned.put(nodes.get(i), numSegments * numOwners / numNodes + 1);
         } else {
            expectedOwned.put(nodes.get(i), numSegments * numOwners / numNodes);
         }
      }

      // Iterate over the segments, remove owners if the segment has more than numOwners owners
      // and the owner has > expectedOwned segments.
      int[] neededNewOwners = new int[numSegments];
      for (int i = numSegments - 1; i >= 0; i--) {
         List<Address> owners = ownerLists[i];
         neededNewOwners[i] = numOwners - owners.size();
         for (int j = owners.size() - 1; j >= 1; j++) {
            if (owners.size() <= numOwners)
               break;

            Address owner = owners.get(j);
            int owned = stats.getOwned(owner);
            if (owned > expectedOwned.get(owner)) {
               // Remove the owner
               owners.remove(owner);
            }
         }
      }
   }

   private Address removeOneOf(Collection<Address> list, Collection<Address> searchFor) {
      Address result = null;
      for (Iterator<Address> it = list.iterator(); it.hasNext(); ) {
         Address element = it.next();
         if (searchFor.contains(element)) {
            result = element;
            it.remove();
            break;
         }
      }
      return result;
   }

   private Address removeNotOneOf(Collection<Address> list, Collection<Address> searchFor) {
      Address result = null;
      for (Iterator<Address> it = list.iterator(); it.hasNext(); ) {
         Address element = it.next();
         if (!searchFor.contains(element)) {
            result = element;
            it.remove();
            break;
         }
      }
      return result;
   }

   private NewConsistentHash removeLeavers(NewConsistentHash baseCH, List<Address> newMembers, NewDefaultConsistentHash baseDCH) {
      int numOwners = baseDCH.getNumOwners();
      int numSegments = baseDCH.getNumSegments();

      // we can assume leavers are far fewer than members, so it makes sense to check for leavers
      List<Address> leavers = new ArrayList<Address>(baseCH.getNodes());
      leavers.removeAll(newMembers);

      // remove leavers
      boolean segmentsWithZeroOwners = false;
      List<Address>[] newOwnerLists = new List[numSegments];
      for (int i = 0; i < numSegments; i++) {
         List<Address> owners = new ArrayList<Address>(baseDCH.locateOwnersForSegment(i));
         owners.removeAll(leavers);
         segmentsWithZeroOwners |= owners.isEmpty();
         newOwnerLists[i] = owners;
      }

      // if there are segments with 0 owners, fix them
      if (segmentsWithZeroOwners) {
         assignSegmentsWithZeroOwners(baseDCH, newMembers, newOwnerLists);
      }
      return new NewDefaultConsistentHash(baseDCH.getHashFunction(), numSegments, numOwners, newMembers,
            newOwnerLists);
   }

   private void assignSegmentsWithZeroOwners(NewDefaultConsistentHash baseDCH, List<Address> newMembers,
                                             List<Address>[] ownerLists) {
      NewDefaultConsistentHashFactory.CHStats stats = computeStatistics(baseDCH, newMembers);
      int numOwners = baseDCH.getNumOwners();
      int numSegments = baseDCH.getNumSegments();
      int maxPrimaryOwnedSegments = (int) Math.ceil((double) numSegments / newMembers.size());
      int maxOwnedSegments = (int) Math.ceil((double) numSegments / newMembers.size() * numOwners);
      for (int i = 0; i < numSegments; i++) {
         if (ownerLists[i].size() == 0) {
            // this segment doesn't have any owners, choose new ones
            List<Address> newOwners = new ArrayList<Address>(numOwners);

            // pick the primary owner first
            // We pick the first node that doesn't "primary own" maxPrimaryOwnedSegments.
            // This algorithm will always a primary owner for a segment, because maxPrimaryOwnedSegments
            // is always recomputed so that numNodes * maxPrimaryOwnedSegments >= numSegments
            // It might leave some nodes with < minPrimaryOwnedSegments, but that's ok at this stage
            for (int j = 0; j < newMembers.size(); j++) {
               Address a = newMembers.get(j);
               if (stats.getPrimaryOwned(a) < maxPrimaryOwnedSegments) {
                  newOwners.add(a);
                  stats.incPrimaryOwned(a);
                  stats.incOwned(a);
                  break;
               }
            }
            if (newOwners.isEmpty()) {
               log.debugf("Could not find a primary owner for segment %d, base ch = %s, new members = %s",
                     i, baseDCH, newMembers);
               throw new IllegalStateException("Could not find a primary owner!");
            }

            // then the backup owners
            // start again from the beginning so that we don't have to wrap around
            if (numOwners > 1) {
               for (int j = 0; j < newMembers.size(); j++) {
                  Address a = newMembers.get(j);
                  if (stats.getOwned(a) < maxOwnedSegments && !newOwners.contains(a)) {
                     newOwners.add(a);
                     stats.incOwned(a);
                     if (newOwners.size() == numOwners)
                        break;
                  }
               }
            }
            // we might have < numOwners owners at this point, if the base CH wasn't properly balanced
            // (i.e. some nodes owned > maxOwnedSegment)
            // but as long as we have at least one owner we're going to be fine
            ownerLists[i] = newOwners;
         }
      }
   }

   private CHStats computeStatistics(NewDefaultConsistentHash ch, List<Address> nodes) {
      CHStats stats = new CHStats(nodes);
      for (int i = 0; i < ch.getNumSegments(); i++) {
         List<Address> owners = ch.locateOwnersForSegment(i);
         for (int j = 0; i < owners.size(); j++) {
            Address address = owners.get(j);
            if (j == 0) {
               stats.incPrimaryOwned(address);
            }
            stats.incOwned(address);
         }
      }
      return stats;
   }

   private static class CHStats {
      final List<Address> nodes;
      final int[] primaryOwned;
      final int[] owned;

      CHStats(List<Address> nodes) {
         this.nodes = nodes;
         this.primaryOwned = new int[nodes.size()];
         this.owned = new int[nodes.size()];
      }

      int getPrimaryOwned(Address a) {
         int i = nodes.indexOf(a);
         if (i < 0) return 0;
         return primaryOwned[i];
      }

      int getOwned(Address a) {
         int i = nodes.indexOf(a);
         if (i < 0) return 0;
         return owned[nodes.indexOf(a)];
      }

      void incPrimaryOwned(Address a) {
         int i = nodes.indexOf(a);
         if (i < 0)
            throw new IllegalArgumentException("Trying to modify statistics for a node that doesn't exist");

         primaryOwned[i]++;
      }

      void incOwned(Address a) {
         int i = nodes.indexOf(a);
         if (i < 0)
            throw new IllegalArgumentException("Trying to modify statistics for a node that doesn't exist");

         owned[i]++;
      }

      void decPrimaryOwned(Address a) {
         int i = nodes.indexOf(a);
         if (i < 0)
            throw new IllegalArgumentException("Trying to modify statistics for a node that doesn't exist");

         primaryOwned[i]--;
      }

      void decOwned(Address a) {
         int i = nodes.indexOf(a);
         if (i < 0)
            throw new IllegalArgumentException("Trying to modify statistics for a node that doesn't exist");

         owned[i]--;
      }
   }
}
