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


import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;

import org.infinispan.CacheException;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.distribution.newch.ConsistentHash;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.Merged;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.util.concurrent.ConcurrentMapFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import static org.infinispan.factories.KnownComponentNames.ASYNC_TRANSPORT_EXECUTOR;

/**
 * The {@code ClusterTopologyManager} implementation.
 *
 * @author Dan Berindei
 * @since 5.2
 */
class ClusterTopologyManagerImpl implements ClusterTopologyManager {
   private static Log log = LogFactory.getLog(LocalTopologyManagerImpl.class);

   private Transport transport;
   private RebalancePolicy rebalancePolicy;
   private GlobalConfiguration globalConfiguration;
   private ExecutorService asyncTransportExecutor;

   private ConcurrentMap<String, CacheJoinInfo> clusterCaches = ConcurrentMapFactory.makeConcurrentMap();

   public void inject(Transport transport, RebalancePolicy rebalancePolicy,
                      @ComponentName(ASYNC_TRANSPORT_EXECUTOR) ExecutorService asyncTransportExecutor,
                      GlobalConfiguration globalConfiguration) {
      this.transport = transport;
      this.rebalancePolicy = rebalancePolicy;
      this.asyncTransportExecutor = asyncTransportExecutor;
      this.globalConfiguration = globalConfiguration;
   }

   @Override
   public void updateConsistentHash(String cacheName, int topologyId, ConsistentHash currentCH,
                                    ConsistentHash pendingCH) throws Exception {
      ReplicableCommand command = new CacheTopologyControlCommand(cacheName,
            CacheTopologyControlCommand.Type.CH_UPDATE, transport.getAddress(), topologyId, currentCH, pendingCH);
      int timeout = clusterCaches.get(cacheName).getTimeout();
      executeEverywhereSync(command, timeout);
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

   private Map<Address, Object> executeEverywhereSync(ReplicableCommand command, int timeout)
         throws Exception {
      Map<Address, Response> responseMap = transport.invokeRemotely(null,
            command, ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS, timeout, false, null);

      Map<Address, Object> responseValues = new HashMap<Address, Object>(transport.getMembers().size());
      for (Map.Entry<Address, Response> entry : responseMap.entrySet()) {
         Address address = entry.getKey();
         Response response = entry.getValue();
         if (!response.isSuccessful()) {
            throw new CacheException("Unsuccessful response received from node " + address + ": " + response);
         }
         responseValues.put(address, ((SuccessfulResponse) response).getResponseValue());
      }
      return responseValues;
   }

   private void executeEverywhereAsync(ReplicableCommand command)
         throws Exception {
      transport.invokeRemotely(null, command, ResponseMode.ASYNCHRONOUS_WITH_SYNC_MARSHALLING, -1, false, null);
   }


   @Listener(sync = false)
   public class ClusterViewListener {
      @Merged
      @ViewChanged
      public void handleViewChange(final ViewChangedEvent e) {
         handleNewView(e.getNewMembers(), e.isMergeView());
      }
   }

   private void handleNewView(List<Address> newMembers, boolean mergeView) {
      if (mergeView) {
         ReplicableCommand command = new CacheTopologyControlCommand(null,
               CacheTopologyControlCommand.Type.GET_STATUS, transport.getAddress());
         // TODO Rename setting to something like globalRpcTimeout
         int timeout = (int) globalConfiguration.transport().distributedSyncTimeout();
         try {
            Map<Address, Object> statusResponses = executeEverywhereSync(command, timeout);

            HashMap<String, List<CacheTopology>> clusterCacheMap = new HashMap<String, List<CacheTopology>>();
            for (Object o : statusResponses.values()) {
               Map<String, CacheTopology> nodeStatus = (Map<String, CacheTopology>) o;
               for (Map.Entry<String, CacheTopology> e : nodeStatus.entrySet()) {
                  String cacheName = e.getKey();
                  CacheTopology cacheTopology = e.getValue();
                  List<CacheTopology> topologyList = clusterCacheMap.get(cacheName);
                  if (topologyList == null) {
                     topologyList = new ArrayList<CacheTopology>();
                     clusterCacheMap.put(cacheName, topologyList);
                  }
                  topologyList.add(cacheTopology);
               }
            }

            for (Map.Entry<Address, Object> e : statusResponses.entrySet()) {
               Address cacheName = e.getKey();
               Object topologyList = e.getValue();
               rebalancePolicy.initCache(cacheName, topologyList);
            }
         } catch (Exception e) {
            //TODO log.errorRecoveringClusterState(e);
            log.errorf(e, "Error recovering cluster state");
         }
      } else {
         rebalancePolicy.updateMembersList(newMembers);
      }
   }
}