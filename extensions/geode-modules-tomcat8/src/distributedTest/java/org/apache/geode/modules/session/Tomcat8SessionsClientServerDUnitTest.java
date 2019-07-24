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
package org.apache.geode.modules.session;

import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;

import java.util.Properties;

import javax.security.auth.message.config.AuthConfigFactory;

import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.springframework.util.SocketUtils;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.modules.session.catalina.ClientServerCacheLifecycleListener;
import org.apache.geode.modules.session.catalina.DeltaSessionManager;
import org.apache.geode.modules.session.catalina.Tomcat8DeltaSessionManager;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.SessionTest;

@Category(SessionTest.class)
public class Tomcat8SessionsClientServerDUnitTest extends TestSessionsTomcat8Base {
  private ClientCache clientCache;

  @Before
  public void setUp() throws Exception {
    vm0 = VM.getVM(1);
    String hostName = vm0.getHost().getHostName();
    int cacheServerPort = vm0.invoke(() -> {
      Properties props = new Properties();
      CacheFactory cf = new CacheFactory(props);
      Cache cache = cf.create();
      CacheServer server = cache.addCacheServer();
      server.setPort(0);
      server.start();

      return server.getPort();
    });

    port = SocketUtils.findAvailableTcpPort();
    server = new EmbeddedTomcat8(port, "JVM-1");

    ClientCacheFactory cacheFactory = new ClientCacheFactory();
    cacheFactory.addPoolServer(hostName, cacheServerPort);
    clientCache = cacheFactory.create();
    DeltaSessionManager manager = new Tomcat8DeltaSessionManager();

    ClientServerCacheLifecycleListener listener = new ClientServerCacheLifecycleListener();
    listener.setProperty(MCAST_PORT, "0");
    listener.setProperty(LOG_LEVEL, "config");
    server.addLifecycleListener(listener);
    sessionManager = manager;
    sessionManager.setEnableCommitValve(true);
    server.getRootContext().setManager(sessionManager);
    AuthConfigFactory.setFactory(null);

    servlet = server.addServlet("/test/*", "default", CommandServlet.class.getName());
    server.startContainer();


    // Can only retrieve the region once the container has started up (& the cache has started too).
    region = sessionManager.getSessionCache().getSessionRegion();
    sessionManager.getTheContext().setSessionTimeout(30);
  }

  @After
  public void tearDown() {
    vm0.invoke(() -> CacheFactory.getAnyInstance().getCacheServers().forEach(CacheServer::stop));

    clientCache.close();
    server.stopContainer();
  }
}
