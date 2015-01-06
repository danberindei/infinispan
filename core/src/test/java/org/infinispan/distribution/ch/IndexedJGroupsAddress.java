package org.infinispan.distribution.ch;

import org.infinispan.remoting.transport.jgroups.JGroupsAddress;

/**
 * We extend JGroupsAddress to make mapping an address to a node easier.
 */
class IndexedJGroupsAddress extends JGroupsAddress {
   final int nodeIndex;

   IndexedJGroupsAddress(org.jgroups.Address address, int nodeIndex) {
      super(address);
      this.nodeIndex = nodeIndex;
   }

   @Override
   public String toString() {
      return "Address#" + nodeIndex;
   }
}
