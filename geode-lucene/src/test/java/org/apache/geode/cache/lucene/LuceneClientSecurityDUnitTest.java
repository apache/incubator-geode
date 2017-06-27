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
package org.apache.geode.cache.lucene;

import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.INDEX_NAME;
import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.REGION_NAME;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_AUTH_INIT;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Properties;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.security.SimpleTestSecurityManager;
import org.apache.geode.security.templates.UserPasswordAuthInit;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category({DistributedTest.class, SecurityTest.class})
@RunWith(JUnitParamsRunner.class)
public class LuceneClientSecurityDUnitTest extends LuceneQueriesAccessorBase {

  @Test
  @Parameters(method = "getSearchIndexUserNameAndExpectedResponses")
  public void verifySearchIndexPermissions(
      LuceneCommandsSecurityTest.UserNameAndExpectedResponse user) {
    // Start server
    int serverPort = dataStore1.invoke(() -> startCacheServer());

    // Start client
    accessor.invoke(() -> startClient(user.getUserName(), serverPort));

    // Attempt query
    accessor.invoke(
        () -> executeTextSearch(user.getExpectAuthorizationError(), user.getExpectedResponse()));
  }

  private int startCacheServer() throws IOException {
    Properties props = new Properties();
    props.setProperty(SECURITY_MANAGER, SimpleTestSecurityManager.class.getName());
    final Cache cache = getCache(props);
    final CacheServer server = cache.addCacheServer();
    server.setPort(0);
    server.start();
    LuceneService luceneService = LuceneServiceProvider.get(cache);
    luceneService.createIndexFactory().addField("text").create(INDEX_NAME, REGION_NAME);
    cache.createRegionFactory(RegionShortcut.PARTITION).create(REGION_NAME);
    return server.getPort();
  }

  private void startClient(String userName, int serverPort) {
    Properties props = new Properties();
    props.setProperty("security-username", userName);
    props.setProperty("security-password", userName);
    props.setProperty(SECURITY_CLIENT_AUTH_INIT, UserPasswordAuthInit.class.getName());
    ClientCacheFactory clientCacheFactory = new ClientCacheFactory(props);
    clientCacheFactory.addPoolServer("localhost", serverPort);
    ClientCache clientCache = getClientCache(clientCacheFactory);
    clientCache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create(REGION_NAME);
  }

  private void executeTextSearch(boolean expectAuthorizationError, String expectedResponse)
      throws LuceneQueryException {
    LuceneService service = LuceneServiceProvider.get(getCache());
    LuceneQuery<Integer, TestObject> query =
        service.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME, "test", "text");
    try {
      query.findKeys();
      assertFalse(expectAuthorizationError);
    } catch (ServerOperationException e) {
      assertTrue(e.getCause() != null && e.getCause() instanceof NotAuthorizedException);
      assertTrue(expectAuthorizationError);
      if (expectedResponse != null) {
        assertTrue(e.getLocalizedMessage().contains(expectedResponse));
      }
    }
  }

  protected LuceneCommandsSecurityTest.UserNameAndExpectedResponse[] getSearchIndexUserNameAndExpectedResponses() {
    return new LuceneCommandsSecurityTest.UserNameAndExpectedResponse[] {
        new LuceneCommandsSecurityTest.UserNameAndExpectedResponse("nopermissions", true),
        new LuceneCommandsSecurityTest.UserNameAndExpectedResponse("datawrite", false)};
  }
}
