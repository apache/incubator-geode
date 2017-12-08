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

package org.apache.geode.cache.query.dunit;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryExecutionTimeoutException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.cq.dunit.CqQueryDUnitTest;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.internal.DefaultQuery;
import org.apache.geode.cache.query.internal.QueryMonitor;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.cache30.CacheTestCase;
import org.apache.geode.cache30.ClientServerTestCase;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.ThreadUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.FlakyTest;

/**
 * Tests for QueryMonitoring service.
 *
 * @since GemFire 6.0
 */
@Category({DistributedTest.class, ClientSubscriptionTest.class})
public class QueryMonitorDUnitTest extends JUnit4CacheTestCase {

  private final String exampleRegionName = "exampleRegion";
  private final String exampleRegionName2 = "exampleRegion2";
  private final String poolName = "serverConnectionPool";


  /* Some of the queries are commented out as they were taking less time */
  String[] queryStr = {"SELECT ID FROM /root/exampleRegion p WHERE  p.ID > 100",
      "SELECT DISTINCT * FROM /root/exampleRegion x, x.positions.values WHERE  x.pk != '1000'",
      "SELECT DISTINCT * FROM /root/exampleRegion x, x.positions.values WHERE  x.pkid != '1'",
      "SELECT DISTINCT * FROM /root/exampleRegion p, p.positions.values WHERE  p.pk > '1'",
      "SELECT DISTINCT * FROM /root/exampleRegion p, p.positions.values WHERE  p.pkid != '53'",
      "SELECT DISTINCT pos FROM /root/exampleRegion p, p.positions.values pos WHERE  pos.Id > 100",
      "SELECT DISTINCT pos FROM /root/exampleRegion p, p.positions.values pos WHERE  pos.Id > 100 and pos.secId IN SET('YHOO', 'IBM', 'AMZN')",
      "SELECT * FROM /root/exampleRegion p WHERE  p.ID > 100 and p.status = 'active' and p.ID < 100000",
      "SELECT * FROM /root/exampleRegion WHERE  ID > 100 and status = 'active'",
      "SELECT DISTINCT * FROM /root/exampleRegion p WHERE  p.ID > 100 and p.status = 'active' and p.ID < 100000",
      "SELECT DISTINCT ID FROM /root/exampleRegion WHERE  status = 'active'",
      "SELECT DISTINCT ID FROM /root/exampleRegion p WHERE  p.status = 'active'",
      "SELECT DISTINCT pos FROM /root/exampleRegion p, p.positions.values pos WHERE  pos.secId IN SET('YHOO', 'IBM', 'AMZN')",
      "SELECT DISTINCT proj1:p, proj2:itrX FROM /root/exampleRegion p, (SELECT DISTINCT pos FROM /root/exampleRegion p, p.positions.values pos"
          + " WHERE  pos.secId = 'YHOO') as itrX",
      "SELECT DISTINCT * FROM /root/exampleRegion p, (SELECT DISTINCT pos FROM /root/exampleRegion p, p.positions.values pos"
          + " WHERE  pos.secId = 'YHOO') as itrX",
      "SELECT DISTINCT * FROM /root/exampleRegion p, (SELECT DISTINCT p.ID FROM /root/exampleRegion x"
          + " WHERE  x.ID = p.ID) as itrX",
      "SELECT DISTINCT * FROM /root/exampleRegion p, (SELECT DISTINCT pos FROM /root/exampleRegion x, x.positions.values pos"
          + " WHERE  x.ID = p.ID) as itrX",
      "SELECT DISTINCT x.ID FROM /root/exampleRegion x, x.positions.values v WHERE  "
          + "v.secId = element(SELECT DISTINCT vals.secId FROM /root/exampleRegion p, p.positions.values vals WHERE  vals.secId = 'YHOO')",
      "SELECT DISTINCT * FROM /root/exampleRegion p, /root/exampleRegion2 p2 WHERE  p.status = 'active'",
      "SELECT DISTINCT p.ID FROM /root/exampleRegion p, /root/exampleRegion2 p2 WHERE  p.ID = p2.ID",
      "SELECT p.ID FROM /root/exampleRegion p, /root/exampleRegion2 p2 WHERE  p.ID = p2.ID and p.status = 'active' and p2.status = 'active'",
      "SELECT p.ID FROM /root/exampleRegion p, /root/exampleRegion2 p2 WHERE  p.ID = p2.ID and p.status = 'active' and p.status = p2.status",
      "SELECT DISTINCT p.ID FROM /root/exampleRegion p, /root/exampleRegion2 p2 WHERE  p.ID = p2.ID and p.ID > 100 and p2.ID < 100000",
      "SELECT p.ID FROM /root/exampleRegion p, /root/exampleRegion2 p2 WHERE  p.ID = p2.ID and p.ID > 100 and p2.ID < 100000 or p.status = p2.status",
      "SELECT p.ID FROM /root/exampleRegion p, /root/exampleRegion2 p2 WHERE  p.ID = p2.ID and p.ID > 100 and p2.ID < 100000 or p.status = 'active'",
      "SELECT DISTINCT * FROM /root/exampleRegion p, positions.values pos WHERE   (p.ID > 1 or p.status = 'active') or (true AND pos.secId ='IBM')",
      "SELECT DISTINCT * FROM /root/exampleRegion p, positions.values pos WHERE   (p.ID > 1 or p.status = 'active') or (true AND pos.secId !='IBM')",
      "SELECT DISTINCT structset.sos, structset.key "
          + "FROM /root/exampleRegion p, p.positions.values outerPos, "
          + "(SELECT DISTINCT key: key, sos: pos.sharesOutstanding "
          + "FROM /root/exampleRegion.entries pf, pf.value.positions.values pos "
          + "where outerPos.secId != 'IBM' AND "
          + "pf.key IN (SELECT DISTINCT * FROM pf.value.collectionHolderMap['0'].arr)) structset "
          + "where structset.sos > 2000",
      "SELECT DISTINCT * " + "FROM /root/exampleRegion p, p.positions.values outerPos, "
          + "(SELECT DISTINCT key: key, sos: pos.sharesOutstanding "
          + "FROM /root/exampleRegion.entries pf, pf.value.positions.values pos "
          + "where outerPos.secId != 'IBM' AND "
          + "pf.key IN (SELECT DISTINCT * FROM pf.value.collectionHolderMap['0'].arr)) structset "
          + "where structset.sos > 2000",
      "SELECT DISTINCT * FROM /root/exampleRegion p, p.positions.values position "
          + "WHERE (true = null OR position.secId = 'SUN') AND true",};

  String[] prQueryStr = {
      "SELECT ID FROM /root/exampleRegion p WHERE  p.ID > 100 and p.status = 'active'",
      "SELECT * FROM /root/exampleRegion WHERE  ID > 100 and status = 'active'",
      "SELECT DISTINCT * FROM /root/exampleRegion p WHERE   p.ID > 100 and p.status = 'active' and p.ID < 100000",
      "SELECT DISTINCT p.ID FROM /root/exampleRegion p WHERE p.ID > 100 and p.ID < 100000 and p.status = 'active'",
      "SELECT DISTINCT * FROM /root/exampleRegion p, positions.values pos WHERE (p.ID > 1 or p.status = 'active') or (pos.secId != 'IBM')",};

  private int numServers;

  @Override
  public final void preTearDownCacheTestCase() throws Exception {
    Host host = Host.getHost(0);
    disconnectFromDS();
    // shut down clients before servers
    for (int i = numServers; i < 4; i++) {
      host.getVM(i).invoke(() -> CacheTestCase.disconnectFromDS());
    }
  }

  public void setup(int numServers) throws Exception {
    Host host = Host.getHost(0);
    this.numServers = numServers;

    // avoid IllegalStateException from HandShake by connecting all vms tor
    // system before creating connection pools
    getSystem();

    for (int i = 0; i < numServers; i++) {
      host.getVM(i).invoke("getSystem", () -> {
        getSystem();
        return 0;
      });
    }

    for (int i = numServers; i < 4; i++) {
      host.getVM(i).invoke("getClientSystem", () -> {
        Properties props = DistributedTestUtils.getAllDistributedSystemProperties(new Properties());
        props.put(LOCATORS, "");
        getSystem(props);
      });
    }
  }

  public void createRegion() {
    createRegion(false, null);
  }

  private void createRegion(final boolean eviction, final String dirName) {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    // setting the eviction attributes.
    if (eviction) {
      File[] f = new File[1];
      f[0] = new File(dirName);
      f[0].mkdir();
      DiskStoreFactory dsf = GemFireCacheImpl.getInstance().createDiskStoreFactory();
      DiskStore ds1 = dsf.setDiskDirs(f).create("ds1");
      factory.setDiskStoreName("ds1");
      EvictionAttributes evictAttrs =
          EvictionAttributes.createLRUEntryAttributes(100, EvictionAction.OVERFLOW_TO_DISK);
      factory.setEvictionAttributes(evictAttrs);
    }
    // Create region
    createRegion(exampleRegionName, factory.create());
    createRegion(exampleRegionName2, factory.create());
  }

  private void createPRRegion() {
    AttributesFactory factory = new AttributesFactory();
    // factory.setDataPolicy(DataPolicy.PARTITION);
    factory
        .setPartitionAttributes((new PartitionAttributesFactory()).setTotalNumBuckets(8).create());

    createRegion(exampleRegionName, factory.create());
    createRegion(exampleRegionName2, factory.create());
    Region exampleRegion = getRootRegion().getSubregion(exampleRegionName);
    exampleRegion.getCache().getLogger().fine("#### CREATING PR REGION....");
  }

  private int configServer(final int queryMonitorTime, final String testName) {
    int port = 0;
    try {
      port = startBridgeServer(0, false);
    } catch (Exception ex) {
      Assert.fail("While starting CacheServer", ex);
    }
    Cache cache = getCache();
    GemFireCacheImpl.getInstance().testMaxQueryExecutionTime = queryMonitorTime;
    cache.getLogger().fine("#### RUNNING TEST : " + testName);
    DefaultQuery.testHook = new QueryTimeoutHook(queryMonitorTime);
    // ((GemFireCache)cache).testMaxQueryExecutionTime = queryMonitorTime;
    System.out.println("MAX_QUERY_EXECUTION_TIME is set to: "
        + ((GemFireCacheImpl) cache).testMaxQueryExecutionTime);
    return port;
  }

  // Stop server
  private void stopServer(VM server) {
    SerializableRunnable stopServer = new SerializableRunnable("Stop CacheServer") {
      public void run() {
        // Reset the test flag.
        Cache cache = getCache();
        DefaultQuery.testHook = null;
        GemFireCacheImpl.getInstance().testMaxQueryExecutionTime = -1;
        stopBridgeServer(getCache());
        System.out.println("MAX_QUERY_EXECUTION_TIME is set to: "
            + ((GemFireCacheImpl) cache).testMaxQueryExecutionTime);
      }
    };
    server.invoke(stopServer);
  }

  private void configClient(String host, int... ports) {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    PoolFactory poolFactory = PoolManager.createFactory();
    poolFactory.setReadTimeout(10 * 60 * 1000); // 10 mins.
    ClientServerTestCase.configureConnectionPoolWithNameAndFactory(factory, host, ports, true, -1,
        -1, null, poolName, poolFactory);
  }

  private void verifyException(Exception e) {
    e.printStackTrace();
    String error = e.getMessage();
    if (e.getCause() != null) {
      error = e.getCause().getMessage();
    }

    if (error.contains("Query execution cancelled after exceeding max execution time")
        || error.contains("The Query completed sucessfully before it got canceled")
        || error.contains("The QueryMonitor thread may be sleeping longer than the set sleep time")
        || error.contains(
            "The query task could not be found but the query is marked as having been canceled")) {
      // Expected exception.
      return;
    }

    System.out.println("Unexpected exception:");
    if (e.getCause() != null) {
      e.getCause().printStackTrace();
    } else {
      e.printStackTrace();
    }

    fail("Expected exception Not found. Expected exception with message: \n"
        + "\"Query execution taking more than the max execution time\"" + "\n Found \n" + error);
  }

  /**
   * Tests query execution from client to server (single server).
   */
  @Test
  public void testQueryMonitorClientServer() throws Exception {

    setup(1);

    final Host host = Host.getHost(0);

    VM server = host.getVM(0);
    VM client1 = host.getVM(1);
    VM client2 = host.getVM(2);
    VM client3 = host.getVM(3);

    final int numberOfEntries = 100;
    String serverHostName = NetworkUtils.getServerHostName(host);

    // Start server
    int serverPort = server.invoke("Create BridgeServer",
        () -> configServer(20, "testQueryMonitorClientServer")); // All the queries taking more than
                                                                 // 20ms should be canceled by Query
                                                                 // monitor.
    server.invoke("createRegion", () -> createRegion());

    // Initialize server regions.
    server.invoke("populatePortfolioRegions", () -> populatePortfolioRegions(numberOfEntries));

    // Initialize Client1 and create client regions.
    client1.invoke("Init client", () -> configClient(serverHostName, serverPort));
    client1.invoke("createRegion", () -> createRegion());

    // Initialize Client2 and create client regions.
    client2.invoke("Init client", () -> configClient(serverHostName, serverPort));
    client2.invoke("createRegion", () -> createRegion());

    // Initialize Client3 and create client regions.
    client3.invoke("Init client", () -> configClient(serverHostName, serverPort));
    client3.invoke("createRegion", () -> createRegion());

    // Execute client queries

    client1.invoke("execute Queries", () -> executeQueriesFromClient(20));
    client2.invoke("execute Queries", () -> executeQueriesFromClient(20));
    client3.invoke("execute Queries", () -> executeQueriesFromClient(20));

    stopServer(server);
  }

  private void executeQueriesFromClient(int timeout) {
    try {
      ClientCache anyInstance = ClientCacheFactory.getAnyInstance();
      ((GemFireCacheImpl) anyInstance).testMaxQueryExecutionTime = timeout;
      Pool pool = PoolManager.find(poolName);
      QueryService queryService = pool.getQueryService();
      executeQueriesAgainstQueryService(queryService);
    } catch (Exception ex) {
      GemFireCacheImpl.getInstance().getLogger().fine("Exception creating the query service", ex);
    }
  }

  private void executeQueriesOnServer() {
    try {
      QueryService queryService = GemFireCacheImpl.getInstance().getQueryService();
      executeQueriesAgainstQueryService(queryService);
    } catch (Exception ex) {
      GemFireCacheImpl.getInstance().getLogger().fine("Exception creating the query service", ex);
    }
  }

  private void executeQueriesAgainstQueryService(QueryService queryService) {
    for (int k = 0; k < queryStr.length; k++) {
      String qStr = queryStr[k];
      executeQuery(queryService, qStr);
    }
  }

  private void executeQuery(QueryService queryService, String qStr) {
    try {
      GemFireCacheImpl.getInstance().getLogger().fine("Executing query :" + qStr);
      Query query = queryService.newQuery(qStr);
      query.execute();
      fail("The query should have been canceled by the QueryMonitor. Query: " + qStr);
    } catch (Exception e) {
      System.out.println("queryStr = " + qStr);
      verifyException(e);
    }
  }

  /**
   * Tests query execution from client to server (multi server).
   */
  @Test
  public void testQueryMonitorMultiClientMultiServer() throws Exception {

    setup(2);

    final Host host = Host.getHost(0);

    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM client1 = host.getVM(2);
    VM client2 = host.getVM(3);

    final int numberOfEntries = 100;

    String serverHostName = NetworkUtils.getServerHostName(host);

    // Start server
    int serverPort1 = server1.invoke("Create BridgeServer",
        () -> configServer(20, "testQueryMonitorMultiClientMultiServer"));// All the queries taking
                                                                          // more than 20ms should
                                                                          // be canceled by Query
                                                                          // monitor.
    server1.invoke("createRegion", () -> createRegion());

    int serverPort2 = server2.invoke("Create BridgeServer",
        () -> configServer(20, "testQueryMonitorMultiClientMultiServer"));// All the queries taking
                                                                          // more than 20ms should
                                                                          // be canceled by Query
                                                                          // monitor.
    server2.invoke("createRegion", () -> createRegion());

    // Initialize server regions.
    server1.invoke("Create Bridge Server", () -> populatePortfolioRegions(numberOfEntries));

    // Initialize server regions.
    server2.invoke("Create Bridge Server", () -> populatePortfolioRegions(numberOfEntries));

    // Initialize Client1 and create client regions.
    client1.invoke("Init client", () -> configClient(serverHostName, serverPort1, serverPort2));
    client1.invoke("createRegion", () -> createRegion());

    // Initialize Client2 and create client regions.
    client2.invoke("Init client", () -> configClient(serverHostName, serverPort1, serverPort2));
    client2.invoke("createRegion", () -> createRegion());

    // Execute client queries

    client1.invoke("executeQueriesFromClient", () -> executeQueriesFromClient(20));
    client2.invoke("executeQueriesFromClient", () -> executeQueriesFromClient(20));

    stopServer(server1);
    stopServer(server2);
  }

  /**
   * Tests query execution on local vm.
   */
  @Category(FlakyTest.class) // GEODE-577: eats exceptions
  @Test
  public void testQueryExecutionLocally() throws Exception {

    setup(2);

    final Host host = Host.getHost(0);

    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);

    final int numberOfEntries = 100;

    // Start server
    server1.invoke("Create BridgeServer", () -> configServer(20, "testQueryExecutionLocally"));// All
                                                                                               // the
                                                                                               // queries
                                                                                               // taking
                                                                                               // more
                                                                                               // than
                                                                                               // 20ms
                                                                                               // should
                                                                                               // be
                                                                                               // canceled
                                                                                               // by
                                                                                               // Query
                                                                                               // monitor.
    server1.invoke("createRegion", () -> createRegion());

    server2.invoke("Create BridgeServer", () -> configServer(20, "testQueryExecutionLocally"));// All
                                                                                               // the
                                                                                               // queries
                                                                                               // taking
                                                                                               // more
                                                                                               // than
                                                                                               // 20ms
                                                                                               // should
                                                                                               // be
                                                                                               // canceled
                                                                                               // by
                                                                                               // Query
                                                                                               // monitor.
    server2.invoke("createRegion", () -> createRegion());

    // Initialize server regions.
    server1.invoke("Create Bridge Server", () -> populatePortfolioRegions(numberOfEntries));

    // Initialize server regions.
    server2.invoke("Create Bridge Server", () -> populatePortfolioRegions(numberOfEntries));

    // Execute server queries

    server1.invoke("execute queries on Server", () -> executeQueriesOnServer());
    server2.invoke("execute queries on Server", () -> executeQueriesOnServer());

    stopServer(server1);
    stopServer(server2);
  }

  /**
   * Tests query execution on local vm.
   */
  @Test
  public void testQueryExecutionLocallyAndCacheOp() throws Exception {

    setup(2);

    final Host host = Host.getHost(0);

    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);

    final int numberOfEntries = 1000;

    // Start server
    server1.invoke("Create BridgeServer", () -> configServer(20, "testQueryExecutionLocally"));// All
                                                                                               // the
                                                                                               // queries
                                                                                               // taking
                                                                                               // more
                                                                                               // than
                                                                                               // 20ms
                                                                                               // should
                                                                                               // be
                                                                                               // canceled
                                                                                               // by
                                                                                               // Query
                                                                                               // monitor.
    server1.invoke("createRegion", () -> createRegion());

    server2.invoke("Create BridgeServer", () -> configServer(20, "testQueryExecutionLocally"));// All
                                                                                               // the
                                                                                               // queries
                                                                                               // taking
                                                                                               // more
                                                                                               // than
                                                                                               // 20ms
                                                                                               // should
                                                                                               // be
                                                                                               // canceled
                                                                                               // by
                                                                                               // Query
                                                                                               // monitor.
    server2.invoke("createRegion", () -> createRegion());

    // Initialize server regions.
    server1.invoke("populatePortfolioRegions", () -> populatePortfolioRegions(numberOfEntries));

    // Initialize server regions.
    server2.invoke("populatePortfolioRegions", () -> populatePortfolioRegions(numberOfEntries));

    // Execute server queries
    SerializableRunnable executeQuery = new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        try {
          QueryService queryService = GemFireCacheImpl.getInstance().getQueryService();
          String qStr =
              "SELECT DISTINCT * FROM /root/exampleRegion p, (SELECT DISTINCT pos FROM /root/exampleRegion x, x.positions.values pos"
                  + " WHERE  x.ID = p.ID) as itrX";
          executeQuery(queryService, qStr);

          // Create index and Perform cache op. Bug#44307
          queryService.createIndex("idIndex", IndexType.FUNCTIONAL, "ID", "/root/exampleRegion");
          queryService.createIndex("statusIndex", IndexType.FUNCTIONAL, "status",
              "/root/exampleRegion");
          Region exampleRegion = getRootRegion().getSubregion(exampleRegionName);
          for (int i = (1 + 100); i <= (numberOfEntries + 200); i++) {
            exampleRegion.put("" + i, new Portfolio(i));
          }

        } catch (Exception ex) {
          Assert.fail("Exception creating the query service", ex);
        }
      }
    };

    server1.invoke(executeQuery);
    server2.invoke(executeQuery);

    stopServer(server1);
    stopServer(server2);
  }

  private void populatePortfolioRegions(int numberOfEntries) {
    Region exampleRegion = getRootRegion().getSubregion(exampleRegionName);
    Region exampleRegion2 = getRootRegion().getSubregion(exampleRegionName2);
    for (int i = (1 + 100); i <= (numberOfEntries + 100); i++) {
      exampleRegion.put("" + i, new Portfolio(i));
    }
    for (int i = (1 + 100); i <= 200; i++) {
      exampleRegion2.put("" + i, new Portfolio(i));
    }
  }

  /**
   * Tests query execution from client to server (multiple server) on Partition Region .
   */
  @Test
  public void testQueryMonitorOnPR() throws Exception {

    setup(2);

    final Host host = Host.getHost(0);

    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM client1 = host.getVM(2);
    VM client2 = host.getVM(3);

    final int numberOfEntries = 100;

    String serverHostName = NetworkUtils.getServerHostName(host);

    // Start server
    int serverPort1 = server1.invoke("configServer",
        () -> configServer(20, "testQueryMonitorMultiClientMultiServerOnPR"));// All the queries
                                                                              // taking more than
                                                                              // 100ms should be
                                                                              // canceled by Query
                                                                              // monitor.
    server1.invoke("createPRRegion", () -> createPRRegion());

    int serverPort2 = server2.invoke("configServer",
        () -> configServer(20, "testQueryMonitorMultiClientMultiServerOnPR"));// All the queries
                                                                              // taking more than
                                                                              // 100ms should be
                                                                              // canceled by Query
                                                                              // monitor.
    server2.invoke("createPRRegion", () -> createPRRegion());

    // Initialize server regions.
    server1.invoke("bulkInsertPorfolio", () -> bulkInsertPorfolio(101, numberOfEntries));

    // Initialize server regions.
    server2.invoke("bulkInsertPorfolio", () -> bulkInsertPorfolio((numberOfEntries + 100),
        (numberOfEntries + numberOfEntries + 100)));

    // Initialize Client1 and create client regions.
    client1.invoke("Init client", () -> configClient(serverHostName, serverPort1));
    client1.invoke("createRegion", () -> createRegion());

    // Initialize Client2 and create client regions.
    client2.invoke("Init client", () -> configClient(serverHostName, serverPort2));
    client2.invoke("createRegion", () -> createRegion());

    // Execute client queries

    client1.invoke("Execute Queries", () -> executeQueriesFromClient(20));
    client2.invoke("Execute Queries", () -> executeQueriesFromClient(20));

    stopServer(server1);
    stopServer(server2);
  }

  private void bulkInsertPorfolio(int startingId, int numberOfEntries) {
    Region exampleRegion = getRootRegion().getSubregion(exampleRegionName);
    Region exampleRegion2 = getRootRegion().getSubregion(exampleRegionName2);
    for (int i = startingId; i <= (numberOfEntries + 100); i++) {
      exampleRegion.put("" + i, new Portfolio(i));
    }
  }

  /**
   * Tests query execution on Partition Region, executes query locally.
   */
  @Test
  public void testQueryMonitorWithLocalQueryOnPR() throws Exception {

    setup(2);

    final Host host = Host.getHost(0);

    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);

    final int numberOfEntries = 100;

    // Start server
    server1.invoke("configServer",
        () -> configServer(20, "testQueryMonitorMultiClientMultiServerOnPR"));// All the queries
                                                                              // taking more than
                                                                              // 100ms should be
                                                                              // canceled by Query
                                                                              // monitor.
    server1.invoke("Create Partition Regions", () -> createPRRegion());

    server2.invoke("configServer",
        () -> configServer(20, "testQueryMonitorMultiClientMultiServerOnPR"));// All the queries
                                                                              // taking more than
                                                                              // 100ms should be
                                                                              // canceled by Query
                                                                              // monitor.
    server2.invoke("Create Partition Regions", () -> createPRRegion());

    // Initialize server regions.
    server1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        bulkInsertPorfolio(101, numberOfEntries);
      }
    });

    // Initialize server regions.
    server2.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        bulkInsertPorfolio((numberOfEntries + 100), (numberOfEntries + numberOfEntries + 100));
      }
    });

    // Execute client queries
    server1.invoke("execute queries on server", () -> executeQueriesOnServer());
    server2.invoke("execute queries on server", () -> executeQueriesOnServer());

    stopServer(server1);
    stopServer(server2);
  }

  /**
   * Tests query execution from client to server (multiple server) with eviction to disk.
   */
  @Ignore("TODO:BUG46770WORKAROUND: test is disabled")
  @Test
  public void testQueryMonitorRegionWithEviction() throws CacheException {

    final Host host = Host.getHost(0);

    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM client1 = host.getVM(2);
    VM client2 = host.getVM(3);

    final int numberOfEntries = 100;

    String serverHostName = NetworkUtils.getServerHostName(host);

    // Start server
    int serverPort1 = server1.invoke("Create BridgeServer",
        () -> configServer(20, "testQueryMonitorRegionWithEviction"));// All the queries taking more
                                                                      // than 20ms should be
                                                                      // canceled by Query monitor.
    server1.invoke("createRegion",
        () -> createRegion(true, "server1_testQueryMonitorRegionWithEviction"));

    int serverPort2 = server2.invoke("Create BridgeServer",
        () -> configServer(20, "testQueryMonitorRegionWithEviction"));// All the queries taking more
                                                                      // than 20ms should be
                                                                      // canceled by Query monitor.
    server2.invoke("createRegion",
        () -> createRegion(true, "server2_testQueryMonitorRegionWithEviction"));

    // Initialize server regions.
    server1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        bulkInsertPorfolio(101, numberOfEntries);
      }
    });

    // Initialize server regions.
    server2.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        bulkInsertPorfolio((numberOfEntries + 100), (numberOfEntries + numberOfEntries + 100));
      }
    });

    // Initialize Client1 and create client regions.
    client1.invoke("Init client", () -> configClient(serverHostName, serverPort1));
    client1.invoke("createRegion", () -> createRegion());

    // Initialize Client2 and create client regions.
    client2.invoke("Init client", () -> configClient(serverHostName, serverPort2));
    client2.invoke("createRegion", () -> createRegion());

    // Execute client queries
    client1.invoke("Execute Queries", () -> executeQueriesFromClient(20));
    client2.invoke("Execute Queries", () -> executeQueriesFromClient(20));

    stopServer(server1);
    stopServer(server2);
  }

  /**
   * Tests query execution on region with indexes.
   */
  @Test
  public void testQueryMonitorRegionWithIndex() throws Exception {

    setup(2);

    final Host host = Host.getHost(0);

    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM client1 = host.getVM(2);
    VM client2 = host.getVM(3);

    final int numberOfEntries = 100;

    String serverHostName = NetworkUtils.getServerHostName(host);

    // Start server
    int serverPort1 =
        server1.invoke("configServer", () -> configServer(20, "testQueryMonitorRegionWithIndex"));// All
                                                                                                  // the
                                                                                                  // queries
                                                                                                  // taking
                                                                                                  // more
                                                                                                  // than
                                                                                                  // 20ms
                                                                                                  // should
                                                                                                  // be
                                                                                                  // canceled
                                                                                                  // by
                                                                                                  // Query
                                                                                                  // monitor.
    server1.invoke("createRegion", () -> createRegion());

    int serverPort2 =
        server2.invoke("configServer", () -> configServer(20, "testQueryMonitorRegionWithIndex"));// All
                                                                                                  // the
                                                                                                  // queries
                                                                                                  // taking
                                                                                                  // more
                                                                                                  // than
                                                                                                  // 20ms
                                                                                                  // should
                                                                                                  // be
                                                                                                  // canceled
                                                                                                  // by
                                                                                                  // Query
                                                                                                  // monitor.
    server2.invoke("createRegion", () -> createRegion());

    // pause(1000);

    // Initialize server regions.
    server1.invoke("Create Indexes", () -> createIndexes(numberOfEntries));

    // Initialize server regions.
    server2.invoke("Create Indexes", () -> createIndexes(numberOfEntries));

    // Initialize Client1 and create client regions.
    client1.invoke("Init client", () -> configClient(serverHostName, serverPort1));
    client1.invoke("createRegion", () -> createRegion());

    // Initialize Client2 and create client regions.
    client2.invoke("Init client", () -> configClient(serverHostName, serverPort2));
    client2.invoke("createRegion", () -> createRegion());

    // Execute client queries
    client1.invoke("executeQueriesFromClient", () -> executeQueriesFromClient(20));
    client2.invoke("executeQueriesFromClient", () -> executeQueriesFromClient(20));

    stopServer(server1);
    stopServer(server2);
  }

  private void createIndexes(int numberOfEntries) {
    Region exampleRegion = getRootRegion().getSubregion(exampleRegionName);
    Region exampleRegion2 = getRootRegion().getSubregion(exampleRegionName2);

    try {
      // create index.
      QueryService cacheQS = GemFireCacheImpl.getInstance().getQueryService();
      cacheQS.createIndex("idIndex", IndexType.FUNCTIONAL, "p.ID", "/root/exampleRegion p");
      cacheQS.createIndex("statusIndex", IndexType.FUNCTIONAL, "p.status", "/root/exampleRegion p");
      cacheQS.createIndex("secIdIndex", IndexType.FUNCTIONAL, "pos.secId",
          "/root/exampleRegion p, p.positions.values pos");
      cacheQS.createIndex("posIdIndex", IndexType.FUNCTIONAL, "pos.Id",
          "/root/exampleRegion p, p.positions.values pos");
      cacheQS.createIndex("pkIndex", IndexType.PRIMARY_KEY, "pk", "/root/exampleRegion");
      cacheQS.createIndex("pkidIndex", IndexType.PRIMARY_KEY, "pkid", "/root/exampleRegion");
      cacheQS.createIndex("idIndex2", IndexType.FUNCTIONAL, "p2.ID", "/root/exampleRegion2 p2");
      cacheQS.createIndex("statusIndex2", IndexType.FUNCTIONAL, "p2.status",
          "/root/exampleRegion2 p2");
      cacheQS.createIndex("secIdIndex2", IndexType.FUNCTIONAL, "pos.secId",
          "/root/exampleRegion2 p2, p2.positions.values pos");
      cacheQS.createIndex("posIdIndex2", IndexType.FUNCTIONAL, "pos.Id",
          "/root/exampleRegion2 p2, p2.positions.values pos");
      cacheQS.createIndex("pkIndex2", IndexType.PRIMARY_KEY, "pk", "/root/exampleRegion2");
      cacheQS.createIndex("pkidIndex2", IndexType.PRIMARY_KEY, "pkid", "/root/exampleRegion2");
    } catch (Exception ex) {
    }
    for (int i = (1 + 100); i <= (numberOfEntries + 100); i++) {
      exampleRegion.put("" + i, new Portfolio(i));
    }
    for (int i = (1 + 100); i <= (200 + 100); i++) {
      exampleRegion2.put("" + i, new Portfolio(i));
    }
  }

  protected CqQueryDUnitTest cqDUnitTest = new CqQueryDUnitTest();

  /**
   * The following CQ test is added to make sure testMaxQueryExecutionTime is reset and is not
   * affecting other query related tests.
   *
   * @throws Exception
   */
  @Test
  public void testCQWithDestroysAndInvalidates() throws Exception {
    setup(1);

    final Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(1);
    VM producerClient = host.getVM(2);

    cqDUnitTest.createServer(server, 0, true);
    final int port = server.invoke(() -> CqQueryDUnitTest.getCacheServerPort());
    final String host0 = NetworkUtils.getServerHostName(server.getHost());

    // Create client.
    cqDUnitTest.createClient(client, port, host0);
    // producer is not doing any thing.
    cqDUnitTest.createClient(producerClient, port, host0);

    final int size = 10;
    final String name = "testQuery_4";
    cqDUnitTest.createValues(server, cqDUnitTest.regions[0], size);

    cqDUnitTest.createCQ(client, name, cqDUnitTest.cqs[4]);
    cqDUnitTest.executeCQ(client, name, true, null);

    // do destroys and invalidates.
    server.invoke(new CacheSerializableRunnable("Create values") {
      public void run2() throws CacheException {
        Cache cache = getCache();
        System.out.println("TEST CQ MAX_QUERY_EXECUTION_TIME is set to: "
            + ((GemFireCacheImpl) cache).testMaxQueryExecutionTime);

        Region region1 = getRootRegion().getSubregion(cqDUnitTest.regions[0]);
        for (int i = 1; i <= 5; i++) {
          region1.destroy(CqQueryDUnitTest.KEY + i);
        }
      }
    });
    for (int i = 1; i <= 5; i++) {
      cqDUnitTest.waitForDestroyed(client, name, CqQueryDUnitTest.KEY + i);
    }
    // recreate the key values from 1 - 5
    cqDUnitTest.createValues(server, cqDUnitTest.regions[0], 5);
    // wait for all creates to arrive.
    for (int i = 1; i <= 5; i++) {
      cqDUnitTest.waitForCreated(client, name, CqQueryDUnitTest.KEY + i);
    }

    // do more puts to push first five key-value to disk.
    cqDUnitTest.createValues(server, cqDUnitTest.regions[0], 10);
    // do invalidates on fisrt five keys.
    server.invoke(new CacheSerializableRunnable("Create values") {
      public void run2() throws CacheException {
        Cache cache = getCache();
        System.out.println("TEST CQ MAX_QUERY_EXECUTION_TIME is set to: "
            + ((GemFireCacheImpl) cache).testMaxQueryExecutionTime);

        Region region1 = getRootRegion().getSubregion(cqDUnitTest.regions[0]);
        for (int i = 1; i <= 5; i++) {
          region1.invalidate(CqQueryDUnitTest.KEY + i);
        }
      }
    });
    // wait for invalidates now.
    for (int i = 1; i <= 5; i++) {
      cqDUnitTest.waitForInvalidated(client, name, CqQueryDUnitTest.KEY + i);
    }

    // Close.
    cqDUnitTest.closeClient(client);
    cqDUnitTest.closeServer(server);

  }

  /**
   * Tests cache operation right after query cancellation.
   */
  @Test
  public void testCacheOpAfterQueryCancel() throws Exception {

    setup(4);

    final Host host = Host.getHost(0);

    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM server3 = host.getVM(2);
    VM server4 = host.getVM(3);

    final int numberOfEntries = 1000;

    // Start server
    server1.invoke("Create BridgeServer", () -> configServer(5, "testQueryExecutionLocally"));
    server1.invoke("Create Partition Regions", () -> createPRRegion());

    server2.invoke("Create BridgeServer", () -> configServer(5, "testQueryExecutionLocally"));
    server2.invoke("Create Partition Regions", () -> createPRRegion());

    server3.invoke("Create BridgeServer", () -> configServer(5, "testQueryExecutionLocally"));
    server3.invoke("Create Partition Regions", () -> createPRRegion());

    server4.invoke("Create BridgeServer", () -> configServer(5, "testQueryExecutionLocally"));
    server4.invoke("Create Partition Regions", () -> createPRRegion());

    server1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        try {
          QueryService queryService = GemFireCacheImpl.getInstance().getQueryService();
          queryService.createIndex("statusIndex", IndexType.FUNCTIONAL, "status",
              "/root/exampleRegion");
          queryService.createIndex("secIdIndex", IndexType.FUNCTIONAL, "pos.secId",
              "/root/exampleRegion p, p.positions.values pos");
        } catch (Exception ex) {
          fail("Failed to create index.");
        }
        Region exampleRegion = getRootRegion().getSubregion(exampleRegionName);
        for (int i = 100; i <= (numberOfEntries); i++) {
          exampleRegion.put("" + i, new Portfolio(i));
        }
      }
    });

    // Initialize server regions.
    AsyncInvocation ai1 =
        server1.invokeAsync(new CacheSerializableRunnable("Create Bridge Server") {
          public void run2() throws CacheException {
            Region exampleRegion = getRootRegion().getSubregion(exampleRegionName);
            for (int j = 0; j < 5; j++) {
              for (int i = 1; i <= (numberOfEntries + 1000); i++) {
                exampleRegion.put("" + i, new Portfolio(i));
              }
            }
            LogWriterUtils.getLogWriter()
                .info("### Completed updates in server1 in testCacheOpAfterQueryCancel");
          }
        });

    AsyncInvocation ai2 =
        server2.invokeAsync(new CacheSerializableRunnable("Create Bridge Server") {
          public void run2() throws CacheException {
            Region exampleRegion = getRootRegion().getSubregion(exampleRegionName);
            for (int j = 0; j < 5; j++) {
              for (int i = (1 + 1000); i <= (numberOfEntries + 2000); i++) {
                exampleRegion.put("" + i, new Portfolio(i));
              }
            }
            LogWriterUtils.getLogWriter()
                .info("### Completed updates in server2 in testCacheOpAfterQueryCancel");
          }
        });

    // Execute server queries
    SerializableRunnable executeQuery = new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        try {
          Region exampleRegion = getRootRegion().getSubregion(exampleRegionName);
          QueryService queryService = GemFireCacheImpl.getInstance().getQueryService();
          String qStr =
              "SELECT DISTINCT * FROM /root/exampleRegion p, p.positions.values pos1, p.positions.values pos"
                  + " where p.ID < pos.sharesOutstanding OR p.ID > 0 OR p.position1.mktValue > 0 "
                  + " OR pos.secId in SET ('SUN', 'IBM', 'YHOO', 'GOOG', 'MSFT', "
                  + " 'AOL', 'APPL', 'ORCL', 'SAP', 'DELL', 'RHAT', 'NOVL', 'HP')"
                  + " order by p.status, p.ID desc";
          for (int i = 0; i < 500; i++) {
            try {
              GemFireCacheImpl.getInstance().getLogger().info("Executing query :" + qStr);
              Query query = queryService.newQuery(qStr);
              query.execute();
            } catch (QueryExecutionTimeoutException qet) {
              LogWriterUtils.getLogWriter()
                  .info("### Got Expected QueryExecutionTimeout exception. " + qet.getMessage());
              if (qet.getMessage().contains("cancelled after exceeding max execution")) {
                LogWriterUtils.getLogWriter().info("### Doing a put operation");
                exampleRegion.put("" + i, new Portfolio(i));
              }
            } catch (Exception e) {
              fail("Exception executing query." + e.getMessage());
            }
          }
          LogWriterUtils.getLogWriter()
              .info("### Completed Executing queries in testCacheOpAfterQueryCancel");
        } catch (Exception ex) {
          Assert.fail("Exception creating the query service", ex);
        }
      }
    };

    AsyncInvocation ai3 = server3.invokeAsync(executeQuery);
    AsyncInvocation ai4 = server4.invokeAsync(executeQuery);

    LogWriterUtils.getLogWriter()
        .info("### Waiting for async threads to join in testCacheOpAfterQueryCancel");
    try {
      ThreadUtils.join(ai1, 5 * 60 * 1000);
      ThreadUtils.join(ai2, 5 * 60 * 1000);
      ThreadUtils.join(ai3, 5 * 60 * 1000);
      ThreadUtils.join(ai4, 5 * 60 * 1000);
    } catch (Exception ex) {
      fail("Async thread join failure");
    }
    LogWriterUtils.getLogWriter()
        .info("### DONE Waiting for async threads to join in testCacheOpAfterQueryCancel");

    validateQueryMonitorThreadCnt(server1, 0, 1000);
    validateQueryMonitorThreadCnt(server2, 0, 1000);
    validateQueryMonitorThreadCnt(server3, 0, 1000);
    validateQueryMonitorThreadCnt(server4, 0, 1000);

    LogWriterUtils.getLogWriter()
        .info("### DONE validating query monitor threads testCacheOpAfterQueryCancel");

    stopServer(server1);
    stopServer(server2);
    stopServer(server3);
    stopServer(server4);
  }

  private void validateQueryMonitorThreadCnt(VM vm, final int threadCount, final int waitTime) {
    SerializableRunnable validateThreadCnt =
        new CacheSerializableRunnable("validateQueryMonitorThreadCnt") {
          public void run2() throws CacheException {
            Cache cache = getCache();
            QueryMonitor qm = ((GemFireCacheImpl) cache).getQueryMonitor();
            if (qm == null) {
              fail("Didn't found query monitor.");
            }
            int waited = 0;
            while (true) {
              if (qm.getQueryMonitorThreadCount() != threadCount) {
                if (waited <= waitTime) {
                  Wait.pause(10);
                  waited += 10;
                  continue;
                } else {
                  fail("Didn't found expected monitoring thread. Expected: " + threadCount
                      + " found :" + qm.getQueryMonitorThreadCount());
                }
              }
              break;
            }
            // ((GemFireCache)cache).testMaxQueryExecutionTime = queryMonitorTime;
          }
        };
    vm.invoke(validateThreadCnt);
  }

  /**
   * Starts a bridge server on the given port, using the given deserializeValues and
   * notifyBySubscription to serve up the given region.
   */
  protected int startBridgeServer(int port, boolean notifyBySubscription) throws IOException {

    Cache cache = getCache();
    CacheServer bridge = cache.addCacheServer();
    bridge.setPort(port);
    bridge.setNotifyBySubscription(notifyBySubscription);
    bridge.start();
    return bridge.getPort();
  }

  /**
   * Stops the bridge server that serves up the given cache.
   */
  private void stopBridgeServer(Cache cache) {
    CacheServer bridge = (CacheServer) cache.getCacheServers().iterator().next();
    bridge.stop();
    assertFalse(bridge.isRunning());
  }

  private class QueryTimeoutHook implements DefaultQuery.TestHook {

    long timeout;

    private QueryTimeoutHook(long timeout) {
      this.timeout = timeout;
    }

    public void doTestHook(String description) {
      if (description.equals("6")) {
        try {
          Thread.sleep(timeout * 2);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
        }
      }
    }

    public void doTestHook(int spot) {
      doTestHook("" + spot);
    }

  }

}
