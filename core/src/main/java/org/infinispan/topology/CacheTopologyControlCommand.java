/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.topology;

import org.infinispan.CacheException;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.newch.ConsistentHash;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * A control command to coordinate the installation of cache views between the members of the cluster.
 * The coordinator will receive REQUEST_JOIN and REQUEST_LEAVE commands from the nodes starting or stopping a cache.
 * It then sends PREPARE_VIEW to all the nodes in the cluster.
 * If all nodes return a successful response, the coordinator then sends a COMMIT_VIEW command to everyone.
 * If there is a failure on one of the nodes, the coordinator cancels the view with a ROLLBACK_VIEW command.
 *
 * @author Dan Berindei <dan@infinispan.org>
 * @since 5.2
 */
public class CacheTopologyControlCommand implements ReplicableCommand {

   public enum Type {
      // a node is requesting to join the cluster
      JOIN,
      // a member is signaling that it wants to leave the cluster
      LEAVE,
      // a member is confirming that it finished the rebalance operation
      REBALANCE_CONFIRM,

      // the coordinator is updating the consistent hash
      CH_UPDATE,
      // the coordinator is starting a rebalance operation
      REBALANCE_START,
      // the coordinator is requesting information about the running caches
      GET_STATUS
   }

   private static final Log log = LogFactory.getLog(CacheTopologyControlCommand.class);

   public static final int COMMAND_ID = 17;

   private LocalTopologyManager localTopologyManager;
   private ClusterTopologyManager clusterTopologyManager;

   private final String cacheName;
   private Type type;
   private Address sender;
   private CacheJoinInfo joinInfo;

   // TODO Maybe create a separate command for topology/CH-related stuff; otherwise just replace with a CacheTopology
   private int topologyId;
   private ConsistentHash currentCH;
   private ConsistentHash pendingCH;

   // For CommandIdUniquenessTest only
   public CacheTopologyControlCommand() {
      this.cacheName = null;
   }

   public CacheTopologyControlCommand(String cacheName) {
      this.cacheName = cacheName;
   }

   public CacheTopologyControlCommand(String cacheName, Type type, Address sender, CacheJoinInfo joinInfo) {
      this.cacheName = cacheName;
      this.type = type;
      this.sender = sender;
      this.joinInfo = joinInfo;
   }

   public CacheTopologyControlCommand(String cacheName, Type type, Address sender, int topologyId,
                                      ConsistentHash currentCH, ConsistentHash pendingCH) {
      this.cacheName = cacheName;
      this.type = type;
      this.sender = sender;
      this.topologyId = topologyId;
      this.currentCH = currentCH;
      this.pendingCH = pendingCH;
   }

   public void init(LocalTopologyManager localTopologyManager, ClusterTopologyManager globalDistributionManager) {
      this.localTopologyManager = localTopologyManager;
      this.clusterTopologyManager = globalDistributionManager;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      final boolean trace = log.isTraceEnabled();
      LogFactory.pushNDC(cacheName, trace);
      try {
         switch (type) {
            // member to coordinator
            case JOIN:
               return clusterTopologyManager.handleJoin(cacheName, sender, joinInfo);
            case LEAVE:
               clusterTopologyManager.handleLeave(cacheName, sender);
              return null;
            case REBALANCE_CONFIRM:
               clusterTopologyManager.handleRebalanceCompleted(cacheName, sender, topologyId);
               return null;

            // coordinator to member
            case CH_UPDATE:
               localTopologyManager.handleConsistentHashUpdate(cacheName, currentCH, pendingCH);
               return null;
            case REBALANCE_START:
               localTopologyManager.handleRebalance(cacheName, topologyId, currentCH);
               return null;
            case GET_STATUS:
               return localTopologyManager.handleStatusRequest();
            default:
               throw new CacheException("Unknown cache topology control command type " + type);
         }
      } catch (Throwable t) {
         log.exceptionHandlingCommand(this, t);
         throw t;
      } finally {
         LogFactory.popNDC(trace);
      }
   }

   public String getCacheName() {
      return cacheName;
   }

   public Address getOrigin() {
      return sender;
   }

   public Type getType() {
      return type;
   }

   public int getTopologyId() {
      return topologyId;
   }

   public ConsistentHash getCurrentCH() {
      return currentCH;
   }

   public ConsistentHash getPendingCH() {
      return pendingCH;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return new Object[]{(byte) type.ordinal(), sender, joinInfo, topologyId, currentCH, pendingCH};
   }

   @Override
   @SuppressWarnings("unchecked")
   public void setParameters(int commandId, Object[] parameters) {
      int i = 0;
      type = Type.values()[(Byte) parameters[i++]];
      sender = (Address) parameters[i++];
      joinInfo = (CacheJoinInfo) parameters[i++];
      topologyId = (Integer) parameters[i++];
      currentCH = (ConsistentHash) parameters[i++];
      pendingCH = (ConsistentHash) parameters[i++];
   }

   @Override
   public String toString() {
      return "CacheTopologyControlCommand{" +
            "cache=" + cacheName +
            ", type=" + type +
            ", sender=" + sender +
            ", joinInfo=" + joinInfo +
            ", topologyId=" + topologyId +
            ", currentCH=" + currentCH +
            ", pendingCH=" + pendingCH +
            '}';
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }
}
