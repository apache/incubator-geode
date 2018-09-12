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
package org.apache.geode.internal.cache.tier.sockets;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

@Category({ClientSubscriptionTest.class})
public class RegisterInterestDistributedTest {

  private MemberVM locator;
  private int locatorPort;
  private MemberVM server;

  @Rule
  public ClusterStartupRule locatorServerStartupRule = new ClusterStartupRule();

  @Before
  public void before() throws Exception {
    locator = locatorServerStartupRule.startLocatorVM(1, new Properties());
    locatorPort = locator.getPort();
    server = locatorServerStartupRule.startServerVM(3, locatorPort);
    createServerRegion(server, RegionShortcut.PARTITION);
  }

  @Test
  public void registerInterestAllKeysShouldRegisterForAllKeys() throws Exception {
    ClientCache clientCache = createClientCache(locatorPort);

    Region region =
        clientCache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create("region");
    region.registerInterestForAllKeys();

    server.invoke(() -> {
      Region regionOnServer = ClusterStartupRule.getCache().getRegion("region");
      regionOnServer.put("some key", "some value");
      regionOnServer.put(new ArrayList(), new ArrayList());
      regionOnServer.put(1, 2);
    });

    await().atMost(10, SECONDS).untilAsserted(() -> assertThat(region.size()).isEqualTo(3));
  }

  @Test
  public void registerInterestAllKeysWithInterestPolicyShouldRegisterForAllKeys() throws Exception {
    ClientCache clientCache = createClientCache(locatorPort);

    Region region =
        clientCache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create("region");
    region.registerInterestForAllKeys(InterestResultPolicy.KEYS);

    server.invoke(() -> {
      Region regionOnServer = ClusterStartupRule.getCache().getRegion("region");
      regionOnServer.put("some key", "some value");
      regionOnServer.put(new ArrayList(), new ArrayList());
      regionOnServer.put(1, 2);
    });

    await().atMost(10, SECONDS).untilAsserted(() -> assertThat(region.size()).isEqualTo(3));
  }

  @Test
  public void nonDurableClientRegisterInterestForAllKeysWithDurableFlagShouldThrowException()
      throws Exception {
    ClientCache clientCache = createClientCache(locatorPort);

    Region region =
        clientCache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create("region");

    assertThatThrownBy(() -> region.registerInterestForAllKeys(InterestResultPolicy.KEYS, true))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Durable flag only applicable for durable clients.");
  }

  @Test
  public void durableClientRegisterInterestAllKeysWithDurableFlagShouldRegisterInterest()
      throws Exception {
    ClientCache clientCache = createClientCache(locatorPort, true);

    Region region =
        clientCache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create("region");
    region.registerInterestForAllKeys(InterestResultPolicy.NONE, true);
    clientCache.readyForEvents();

    server.invoke(() -> {
      Region regionOnServer = ClusterStartupRule.getCache().getRegion("region");
      regionOnServer.put("some key", "some value");
      regionOnServer.put(new ArrayList(), new ArrayList());
      regionOnServer.put(1, 2);
    });

    await().atMost(10, SECONDS).untilAsserted(() -> assertThat(region.size()).isEqualTo(3));
  }

  @Test
  public void durableClientRegisterInterestAllKeysAndReceiveValuesFalseShouldRegisterForAllKeys()
      throws Exception {
    ClientCache clientCache = createClientCache(locatorPort, true);

    Region region =
        clientCache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create("region");
    region.registerInterestForAllKeys(InterestResultPolicy.NONE, true, false);
    clientCache.readyForEvents();

    server.invoke(() -> {
      Region regionOnServer = ClusterStartupRule.getCache().getRegion("region");
      regionOnServer.put("some key", "some value");
      regionOnServer.put(new ArrayList(), new ArrayList());
      regionOnServer.put(1, 2);
    });

    await().atMost(10, SECONDS).untilAsserted(() -> assertThat(region.size()).isEqualTo(3));
  }

  @Test
  public void registerInterestForKeysShouldRegisterInterestForEachObjectInTheIterable()
      throws Exception {
    ClientCache clientCache = createClientCache(locatorPort);

    Set keysList = new HashSet();
    keysList.add("some key");
    keysList.add(1);

    Region region =
        clientCache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create("region");
    region.registerInterestForKeys(keysList);

    server.invoke(() -> {
      Region regionOnServer = ClusterStartupRule.getCache().getRegion("region");
      regionOnServer.put("some key", "some value");
      regionOnServer.put(1, 2);
      regionOnServer.put("should not be interested", "in this key/value");
    });

    await().atMost(10, SECONDS).untilAsserted(() -> assertThat(region.size()).isEqualTo(2));
  }

  @Test
  public void registerInterestForKeysWithInterestPolicyShouldRegisterInterestForEachObjectInTheIterable()
      throws Exception {
    ClientCache clientCache = createClientCache(locatorPort);

    Set keysList = new HashSet();
    keysList.add("some key");
    keysList.add(1);

    Region region =
        clientCache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create("region");
    region.registerInterestForKeys(keysList, InterestResultPolicy.KEYS);

    server.invoke(() -> {
      Region regionOnServer = ClusterStartupRule.getCache().getRegion("region");
      regionOnServer.put("some key", "some value");
      regionOnServer.put(1, 2);
      regionOnServer.put("should not be interested", "in this key/value");
    });

    await().atMost(10, SECONDS).untilAsserted(() -> assertThat(region.size()).isEqualTo(2));
  }

  @Test
  public void registerInterestForKeysOnTypedRegionShouldRegisterInterestForEachObjectInIterable()
      throws Exception {
    ClientCache clientCache = createClientCache(locatorPort);

    Set<String> keysList = new HashSet<>();
    keysList.add("some key");
    keysList.add("other key");

    Region<String, String> region =
        clientCache.<String, String>createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY)
            .create("region");
    region.registerInterestForKeys(keysList);

    server.invoke(() -> {
      Region regionOnServer = ClusterStartupRule.getCache().getRegion("region");
      regionOnServer.put("some key", "some value");
      regionOnServer.put("other key", "other value");
      regionOnServer.put("should not be interested", "in this key/value");
    });

    await().atMost(10, SECONDS).untilAsserted(() -> assertThat(region.size()).isEqualTo(2));
  }

  @Test
  public void nonDurableClientWhenRegisterInterestForKeysShouldThrowException() throws Exception {
    ClientCache clientCache = createClientCache(locatorPort);

    Set keysList = new HashSet();
    keysList.add("some key");
    keysList.add(1);

    Region region =
        clientCache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create("region");

    assertThatThrownBy(
        () -> region.registerInterestForKeys(keysList, InterestResultPolicy.KEYS, true))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("Durable flag only applicable for durable clients.");
  }

  @Test
  public void durableClientWhenRegisterInterestForKeyShouldCorrectlyRegisterInterest()
      throws Exception {
    ClientCache clientCache = createClientCache(locatorPort, true);

    Set keysList = new HashSet();
    keysList.add("some key");
    keysList.add(1);

    Region region =
        clientCache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create("region");
    region.registerInterestForKeys(keysList, InterestResultPolicy.KEYS, true);
    clientCache.readyForEvents();

    server.invoke(() -> {
      Region regionOnServer = ClusterStartupRule.getCache().getRegion("region");
      regionOnServer.put("some key", "some value");
      regionOnServer.put(1, 2);
      regionOnServer.put("should not be interested", "in this key/value");
    });

    await().atMost(10, SECONDS).untilAsserted(() -> assertThat(region.size()).isEqualTo(2));
  }

  @Test
  public void durableClientWhenRegisterInterestForKeysAndReturnValueFalseShouldCorrectlyRegisterInterest()
      throws Exception {
    ClientCache clientCache = createClientCache(locatorPort, true);

    Set keysList = new HashSet();
    keysList.add("some key");
    keysList.add(1);

    Region region =
        clientCache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create("region");
    region.registerInterestForKeys(keysList, InterestResultPolicy.KEYS, true, false);
    clientCache.readyForEvents();

    server.invoke(() -> {
      Region regionOnServer = ClusterStartupRule.getCache().getRegion("region");
      regionOnServer.put("some key", "some value");
      regionOnServer.put(1, 2);
      regionOnServer.put("should not be interested", "in this key/value");
    });

    await().atMost(10, SECONDS).untilAsserted(() -> assertThat(region.size()).isEqualTo(2));
  }

  @Test
  public void readyForEventsBeforeAnyPoolsAreCreatedShouldNotResultInIllegalStateException()
      throws Exception {
    ClientCache clientCache = createClientCache(locatorPort, true);

    assertThatCode(() -> clientCache.readyForEvents()).doesNotThrowAnyException();
  }

  private ClientCache createClientCache(Integer locatorPort) {
    return createClientCache(locatorPort, false);
  }

  private ClientCache createClientCache(Integer locatorPort, boolean isDurable) {
    ClientCacheFactory ccf;
    if (isDurable) {
      Properties props = new Properties();
      props.setProperty("durable-client-id", "31");
      props.setProperty("durable-client-timeout", "" + 200);
      ccf = new ClientCacheFactory(props);
    } else {
      ccf = new ClientCacheFactory();
    }

    ccf.addPoolLocator("localhost", locatorPort);
    ccf.setPoolSubscriptionEnabled(true);
    ClientCache cache = ccf.create();
    return cache;
  }

  @Test
  public void testInvalidateReceivedByClientAfterLRUEvictionOfPreviousNullValue() throws Exception {
    ClientCache clientCache = createClientCache(locatorPort);
    Region region =
        clientCache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).addCacheListener(
            new CacheListener<Object, Object>() {
              @Override
              public void afterCreate(EntryEvent<Object, Object> event) {
                log("create key:" + event.getKey());
              }

              @Override
              public void afterUpdate(EntryEvent<Object, Object> event) {
                log("update key:" + event.getKey());

              }

              @Override
              public void afterInvalidate(EntryEvent<Object, Object> event) {
                log("invalidate key:" + event.getKey());

              }

              @Override
              public void afterDestroy(EntryEvent<Object, Object> event) {
                log("destroy key:" + event.getKey());

              }

              @Override
              public void afterRegionInvalidate(RegionEvent<Object, Object> event) {

              }

              @Override
              public void afterRegionDestroy(RegionEvent<Object, Object> event) {

              }

              @Override
              public void afterRegionClear(RegionEvent<Object, Object> event) {

              }

              @Override
              public void afterRegionCreate(RegionEvent<Object, Object> event) {

              }

              @Override
              public void afterRegionLive(RegionEvent<Object, Object> event) {

              }

              private void log(String logString) {
                System.out.println("JASON log:" + logString);
              }
            }).setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(1, EvictionAction.LOCAL_DESTROY)).create("region");
    region.registerInterestForAllKeys();


    server.invoke(() -> {
      Region regionOnServer = ClusterStartupRule.getCache().getRegion("region");
      regionOnServer.putIfAbsent("1", null);
      regionOnServer.putIfAbsent("2", "hello");
      regionOnServer.putIfAbsent("3", "world");

    });

    Thread.sleep(5000);

    region.invalidate("1");

    assertEquals(true, region.containsKey("1"));
  }

  private void createServerRegion(MemberVM server, RegionShortcut regionShortcut) {
    server.invoke(() -> {
      ClusterStartupRule.getCache().createRegionFactory(regionShortcut).create("region");
    });
  }
}
