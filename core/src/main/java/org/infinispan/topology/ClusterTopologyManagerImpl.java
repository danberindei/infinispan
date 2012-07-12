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

package org.infinispan.topology;


import org.infinispan.distribution.newch.ConsistentHash;
import org.infinispan.remoting.transport.Address;


class ClusterTopologyManagerImpl implements ClusterTopologyManager {
   @Override
   public void updateConsistentHash(String cacheName, ConsistentHash currentCH, ConsistentHash balancedCH) {
      //To change body of implemented methods use File | Settings | File Templates.
   }

   @Override
   public void rebalance(String cacheName, int topologyId, ConsistentHash pendingCH) {
      //To change body of implemented methods use File | Settings | File Templates.
   }

   @Override
   public CacheTopology handleJoin(String cacheName, Address joiner, CacheJoinInfo joinInfo) {
      return null;  //To change body of implemented methods use File | Settings | File Templates.
   }

   @Override
   public void handleLeave(String cacheName, Address leaver) {
      //To change body of implemented methods use File | Settings | File Templates.
   }

   @Override
   public void handleRebalanceCompleted(String cacheName, Address node, int topologyId) {
      //To change body of implemented methods use File | Settings | File Templates.
   }
}