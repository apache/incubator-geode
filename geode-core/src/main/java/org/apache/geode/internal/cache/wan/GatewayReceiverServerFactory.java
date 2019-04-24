/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.cache.wan;

import static org.apache.geode.internal.net.SocketCreatorFactory.getSocketCreatorForComponent;
import static org.apache.geode.internal.security.SecurableCommunicationChannel.GATEWAY;

import java.io.IOException;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.distributed.internal.DistributionAdvisee;
import org.apache.geode.internal.cache.CacheServerAdvisor;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalCacheServer;
import org.apache.geode.internal.cache.tier.Acceptor;
import org.apache.geode.internal.cache.tier.OverflowAttributes;
import org.apache.geode.internal.cache.tier.sockets.AcceptorFactory;
import org.apache.geode.internal.cache.tier.sockets.AcceptorImpl;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier.CacheClientNotifierProvider;
import org.apache.geode.internal.cache.tier.sockets.ClientHealthMonitor;
import org.apache.geode.internal.cache.tier.sockets.ClientHealthMonitor.ClientHealthMonitorProvider;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.security.SecurityService;

public class GatewayReceiverServerFactory {

  private final InternalCache cache;
  private final SecurityService securityService;
  private final GatewayReceiverAcceptorFactory acceptorFactory;
  private final GatewayReceiver gatewayReceiver;
  private final GatewayReceiverMetrics gatewayReceiverMetrics;
  private final Supplier<SocketCreator> socketCreatorSupplier;
  private final CacheClientNotifierProvider cacheClientNotifierProvider;
  private final ClientHealthMonitorProvider clientHealthMonitorProvider;
  private final Function<DistributionAdvisee, CacheServerAdvisor> cacheServerAdvisorProvider;

  public GatewayReceiverServerFactory(final InternalCache cache,
      final SecurityService securityService,
      final GatewayReceiver gatewayReceiver,
      final GatewayReceiverMetrics gatewayReceiverMetrics) {
    this(cache, securityService, gatewayReceiver, gatewayReceiverMetrics,
        () -> getSocketCreatorForComponent(GATEWAY), CacheClientNotifier.singletonProvider(),
        ClientHealthMonitor.singletonProvider(), CacheServerAdvisor::createCacheServerAdvisor);
  }

  @VisibleForTesting
  public GatewayReceiverServerFactory(final InternalCache cache,
      final SecurityService securityService,
      final GatewayReceiver gatewayReceiver,
      final GatewayReceiverMetrics gatewayReceiverMetrics,
      final Supplier<SocketCreator> socketCreatorSupplier,
      final CacheClientNotifierProvider cacheClientNotifierProvider,
      final ClientHealthMonitorProvider clientHealthMonitorProvider,
      final Function<DistributionAdvisee, CacheServerAdvisor> cacheServerAdvisorProvider) {
    this.cache = cache;
    this.securityService = securityService;
    acceptorFactory = new GatewayReceiverAcceptorFactory();
    this.gatewayReceiver = gatewayReceiver;
    this.gatewayReceiverMetrics = gatewayReceiverMetrics;
    this.socketCreatorSupplier = socketCreatorSupplier;
    this.cacheClientNotifierProvider = cacheClientNotifierProvider;
    this.clientHealthMonitorProvider = clientHealthMonitorProvider;
    this.cacheServerAdvisorProvider = cacheServerAdvisorProvider;
  }

  public InternalCacheServer createServer() {
    acceptorFactory.setGatewayReceiverEndpoint(this);
    return new CacheServerImpl(cache, securityService, acceptorFactory, false, false,
        socketCreatorSupplier, cacheClientNotifierProvider, clientHealthMonitorProvider,
        cacheServerAdvisorProvider);
  }

  private static class GatewayReceiverAcceptorFactory implements AcceptorFactory {

    private InternalCacheServer internalCacheServer;
    private GatewayReceiverServerFactory gatewayReceiverEndpoint;

    @Override
    public void accept(InternalCacheServer internalCacheServer) {
      this.internalCacheServer = internalCacheServer;
    }

    void setGatewayReceiverEndpoint(GatewayReceiverServerFactory gatewayReceiverEndpoint) {
      this.gatewayReceiverEndpoint = gatewayReceiverEndpoint;
    }

    @Override
    public Acceptor create(OverflowAttributes overflowAttributes) throws IOException {
      return new AcceptorImpl(internalCacheServer.getPort(), internalCacheServer.getBindAddress(),
          internalCacheServer.getNotifyBySubscription(), internalCacheServer.getSocketBufferSize(),
          internalCacheServer.getMaximumTimeBetweenPings(), internalCacheServer.getCache(),
          internalCacheServer.getMaxConnections(), internalCacheServer.getMaxThreads(),
          internalCacheServer.getMaximumMessageCount(), internalCacheServer.getMessageTimeToLive(),
          internalCacheServer.connectionListener(), overflowAttributes,
          internalCacheServer.getTcpNoDelay(), internalCacheServer.serverConnectionFactory(),
          internalCacheServer.timeLimitMillis(), internalCacheServer.securityService(),
          internalCacheServer.socketCreatorSupplier(),
          internalCacheServer.cacheClientNotifierProvider(),
          internalCacheServer.clientHealthMonitorProvider(),
          gatewayReceiverEndpoint.gatewayReceiver,
          gatewayReceiverEndpoint.gatewayReceiverMetrics,
          gatewayReceiverEndpoint.gatewayReceiver.getGatewayTransportFilters());
    }
  }
}
