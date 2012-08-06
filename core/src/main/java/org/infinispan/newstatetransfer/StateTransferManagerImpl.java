/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.newstatetransfer;

import org.infinispan.Cache;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.DefaultConsistentHashFactory;
import org.infinispan.distribution.ch.ReplicatedConsistentHashFactory;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.topology.CacheJoinInfo;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.CacheTopologyHandler;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.util.concurrent.AggregatingNotifyingFutureBuilder;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.ExecutorService;

import static org.infinispan.factories.KnownComponentNames.ASYNC_TRANSPORT_EXECUTOR;

/**
 * @author anistor@redhat.com
 * @since 5.2
 */
public class StateTransferManagerImpl implements StateTransferManager {

   private static final Log log = LogFactory.getLog(StateTransferManagerImpl.class);
   private static final boolean trace = log.isTraceEnabled();

   private final AggregatingNotifyingFutureBuilder statePushFuture = new AggregatingNotifyingFutureBuilder(null, 1);
   private StateTransferLock stateTransferLock;

   private Configuration configuration;
   private DistributionManager distributionManager;
   private LocalTopologyManager localTopologyManager;
   private RpcManager rpcManager;
   private String cacheName;

   private int topologyId;
   private ConsistentHash currentCh = null;

   private StateProvider stateProvider;
   private StateConsumer stateConsumer;

   public StateTransferManagerImpl() {
   }

   @Inject
   public void init(@ComponentName(ASYNC_TRANSPORT_EXECUTOR) ExecutorService asyncTransportExecutor,   //todo [anistor] use a separate ExecutorService
                    Configuration configuration,
                    RpcManager rpcManager,
                    CommandsFactory commandsFactory,
                    DistributionManager distributionManager,
                    CacheLoaderManager cacheLoaderManager,
                    DataContainer dataContainer,
                    InterceptorChain interceptorChain,
                    TransactionTable transactionTable,
                    LocalTopologyManager localTopologyManager,
                    StateTransferLock stateTransferLock,
                    Cache cache,
                    InvocationContextContainer icc) {
      this.configuration = configuration;
      this.distributionManager = distributionManager;
      this.localTopologyManager = localTopologyManager;
      this.rpcManager = rpcManager;
      this.stateTransferLock = stateTransferLock;
      cacheName = cache.getName();

      stateProvider = new StateProviderImpl(asyncTransportExecutor,
            configuration,
            rpcManager,
            commandsFactory,
            cacheLoaderManager,
            dataContainer,
            transactionTable,
            stateTransferLock);

      stateConsumer = new StateConsumerImpl(interceptorChain,
            icc,
            configuration,
            rpcManager,
            commandsFactory,
            cacheLoaderManager,
            dataContainer,
            transactionTable);
   }

   // needs to be AFTER the DistributionManager and *after* the cache loader manager (if any) inits and preloads
   @Start(priority = 60)
   private void start() throws Exception {
      if (trace) {
         log.tracef("Starting state transfer manager on " + rpcManager.getAddress());
      }

      CacheJoinInfo joinInfo = new CacheJoinInfo(
            configuration.clustering().cacheMode().isReplicated()
                  ? new ReplicatedConsistentHashFactory() : new DefaultConsistentHashFactory(),
            configuration.clustering().hash().hash(),
            configuration.clustering().hash().numVirtualNodes(), //todo [anistor] rename to numSegments
            configuration.clustering().hash().numOwners(), configuration.clustering().stateTransfer().timeout());

      CacheTopologyHandler handler = new CacheTopologyHandler() {
         @Override
         public void updateConsistentHash(CacheTopology cacheTopology) {
            rebalance(cacheTopology);
         }

         @Override
         public void rebalance(CacheTopology cacheTopology) {
            distributionManager.setCacheTopology(cacheTopology);
            ConsistentHash ch = cacheTopology.getWriteConsistentHash();
            onTopologyUpdate(topologyId, ch);
         }
      };

      CacheTopology cacheTopology = localTopologyManager.join(cacheName, joinInfo, handler);
      handler.updateConsistentHash(cacheTopology);
   }

   @Stop(priority = 20)
   public void stop() {
      stateProvider.shutdown();
      stateConsumer.shutdown();
      localTopologyManager.leave(cacheName);
   }

   @Override
   public StateProvider getStateProvider() {
      return stateProvider;
   }

   @Override
   public StateConsumer getStateConsumer() {
      return stateConsumer;
   }

   @Override
   public void onTopologyUpdate(int topologyId, ConsistentHash newCh) {
      currentCh = newCh;
      stateProvider.onTopologyUpdate(topologyId, newCh);
      stateTransferLock.setStateTransferInProgress(true);
      stateConsumer.onTopologyUpdate(topologyId, newCh);
   }

   @Override
   public boolean isJoinComplete() {
      return currentCh != null;
   }

   @Override
   public boolean isStateTransferInProgress() {
      return stateConsumer.isStateTransferInProgress();
   }

   @Override
   public boolean isStateTransferInProgressForKey(Object key) {
      return stateConsumer.isStateTransferInProgressForKey(key);
   }

   @Override
   public int getTopologyId() {
      return topologyId;
   }
}