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
package org.apache.geode.internal.cache.partitioned.fixed;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.apache.geode.cache.FixedPartitionAttributes.createFixedPartition;
import static org.apache.geode.cache.RegionShortcut.PARTITION;
import static org.apache.geode.distributed.ConfigurationProperties.MEMBER_TIMEOUT;
import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.apache.geode.test.dunit.VM.getController;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.rules.DistributedRule.getDistributedSystemProperties;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.apache.commons.collections.CollectionUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.EntryOperation;
import org.apache.geode.cache.FixedPartitionAttributes;
import org.apache.geode.cache.FixedPartitionResolver;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.ServerLauncher;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.partitioned.RegionAdvisor;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedCloseableReference;
import org.apache.geode.test.dunit.rules.DistributedMap;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.PartitioningTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;

@Category(PartitioningTest.class)
@SuppressWarnings("serial")
public class FixedPartitioningHADistributedTest implements Serializable {

  private static final String REGION_NAME = "prDS";

  private static final int MEMORY_ACCESSOR = 0;
  private static final int MEMORY_DATASTORE = 10;
  private static final int REDUNDANT_COPIES = 2;
  private static final int TOTAL_NUM_BUCKETS = 8;

  private static final int PARTITIONS = 4;
  private static final int BUCKETS = 10;
  private static final int COUNT = PARTITIONS * BUCKETS;

  private static final Map<Integer, PartitionBucket> BUCKET_TO_PARTITION =
      initialize(new HashMap<>());

  private static final FixedPartitionAttributes Q1_PRIMARY =
      createFixedPartition("Partition-1", true, 10);
  private static final FixedPartitionAttributes Q2_PRIMARY =
      createFixedPartition("Partition-2", true, 10);
  private static final FixedPartitionAttributes Q3_PRIMARY =
      createFixedPartition("Partition-3", true, 10);
  private static final FixedPartitionAttributes Q4_PRIMARY =
      createFixedPartition("Partition-4", true, 10);

  private static final FixedPartitionAttributes Q1_SECONDARY =
      createFixedPartition("Partition-1", false, 10);
  private static final FixedPartitionAttributes Q2_SECONDARY =
      createFixedPartition("Partition-2", false, 10);
  private static final FixedPartitionAttributes Q3_SECONDARY =
      createFixedPartition("Partition-3", false, 10);
  private static final FixedPartitionAttributes Q4_SECONDARY =
      createFixedPartition("Partition-4", false, 10);

  private VM accessor1VM;
  private VM server1VM;
  private VM server2VM;
  private VM server3VM;
  private VM server4VM;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();
  @Rule
  public DistributedCloseableReference<ServerLauncher> serverLauncher =
      new DistributedCloseableReference<>();
  @Rule
  public DistributedMap<VM, List<FixedPartitionAttributes>> fpaMap = new DistributedMap<>();
  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Before
  public void setUp() {
    accessor1VM = getController();
    server1VM = getVM(0);
    server2VM = getVM(1);
    server3VM = getVM(2);
    server4VM = getVM(3);

    fpaMap.put(accessor1VM, emptyList());
    fpaMap.put(server1VM, asList(Q1_PRIMARY, Q2_SECONDARY, Q3_SECONDARY));
    fpaMap.put(server2VM, asList(Q2_PRIMARY, Q3_SECONDARY, Q4_SECONDARY));
    fpaMap.put(server3VM, asList(Q3_PRIMARY, Q4_SECONDARY, Q1_SECONDARY));
    fpaMap.put(server4VM, asList(Q4_PRIMARY, Q1_SECONDARY, Q2_SECONDARY));

    for (VM vm : asList(server1VM, server2VM, server3VM, server4VM)) {
      vm.invoke(() -> startServer(vm, ServerType.DATASTORE));
    }

    startServer(accessor1VM, ServerType.ACCESSOR);
  }

  @Test
  public void recoversAfterBouncingOneDatastore() {
    accessor1VM.invokeAsync(() -> {
      Region<Object, Object> region = serverLauncher.get().getCache().getRegion(REGION_NAME);
      for (int i = 1; i <= COUNT; i++) {
        region.put(partitionBucket(i), value(i));
      }

      // validateBucketsAreFullyRedundant();
    });

    server2VM.bounceForcibly();

    server2VM.invoke(() -> {
      startServer(server2VM, ServerType.DATASTORE);
      //
      // serverLauncher.get().getCache().getResourceManager()
      // .createRestoreRedundancyOperation()
      // .includeRegions(singleton(REGION_NAME))
      // .start()
      // .get(getTimeout().toMinutes(), MINUTES);
      //
      // validateBucketsAreFullyRedundant();
    });

    accessor1VM.invokeAsync(() -> {
      Region<Object, Object> region = serverLauncher.get().getCache().getRegion(REGION_NAME);
      region.put(partitionBucket(20), value(100));

      validateBucketsAreFullyRedundant();
    });
  }

  private void startServer(VM vm, ServerType serverType) {
    String name = serverType.name(vm.getId());
    serverLauncher.set(new ServerLauncher.Builder()
        .set(getDistributedSystemProperties())
        .set(MEMBER_TIMEOUT, "2000")
        .set(SERIALIZABLE_OBJECT_FILTER, getClass().getName() + '*')
        .setDisableDefaultServer(true)
        .setMemberName(name)
        .setWorkingDirectory(folder(name).getAbsolutePath())
        .build());

    serverLauncher.get().start();

    PartitionAttributesFactory<PartitionBucket, Object> partitionAttributesFactory =
        new PartitionAttributesFactory<PartitionBucket, Object>()
            .setLocalMaxMemory(serverType.localMaxMemory())
            .setPartitionResolver(new PartitionBucketResolver())
            .setRedundantCopies(REDUNDANT_COPIES)
            .setTotalNumBuckets(TOTAL_NUM_BUCKETS);

    List<FixedPartitionAttributes> fpaList = fpaMap.get(vm);
    if (CollectionUtils.isNotEmpty(fpaList)) {
      fpaList.forEach(fpa -> partitionAttributesFactory.addFixedPartitionAttributes(fpa));
    }

    serverLauncher.get().getCache()
        .createRegionFactory(PARTITION)
        .setPartitionAttributes(partitionAttributesFactory.create())
        .create(REGION_NAME);
  }

  private void validateBucketsAreFullyRedundant() {
    Region<Object, Object> region = serverLauncher.get().getCache().getRegion(REGION_NAME);
    PartitionedRegion partitionedRegion = (PartitionedRegion) region;
    RegionAdvisor regionAdvisor = partitionedRegion.getRegionAdvisor();

    for (int i = 0; i < TOTAL_NUM_BUCKETS; i++) {
      assertThat(regionAdvisor.getBucketRedundancy(i)).isEqualTo(REDUNDANT_COPIES);
    }
  }

  private File folder(String name) {
    File folder = new File(temporaryFolder.getRoot(), name);
    if (!folder.exists()) {
      assertThat(folder.mkdirs()).isTrue();
    }
    return folder;
  }

  private static Map<Integer, PartitionBucket> initialize(Map<Integer, PartitionBucket> map) {
    for (int i = 1; i <= COUNT; i++) {
      map.put(i, new PartitionBucket(i));
    }
    return map;
  }

  private static PartitionBucket partitionBucket(int partitionBucket) {
    return BUCKET_TO_PARTITION.get(partitionBucket);
  }

  private static String value(int i) {
    return "value-" + i;
  }

  private enum ServerType {
    ACCESSOR(MEMORY_ACCESSOR, id -> "accessor1"),
    DATASTORE(MEMORY_DATASTORE, id -> "datastore" + id);

    private final int localMaxMemory;
    private final Function<Integer, String> nameFunction;

    ServerType(int localMaxMemory, Function<Integer, String> nameFunction) {
      this.localMaxMemory = localMaxMemory;
      this.nameFunction = nameFunction;
    }

    int localMaxMemory() {
      return localMaxMemory;
    }

    String name(int id) {
      return nameFunction.apply(id);
    }
  }

  public static class PartitionBucket implements Serializable {

    private final int partitionBucket;
    private final String partitionName;

    private PartitionBucket(int partitionBucket) {
      this.partitionBucket = partitionBucket;
      int partition = (partitionBucket + BUCKETS - 1) / BUCKETS;
      assertThat(partition > 0 && partition < PARTITIONS + 1);
      partitionName = "Partition-" + partition;
    }

    String getPartitionName() {
      return partitionName;
    }

    @Override
    public String toString() {
      return "Partition-" + partitionName + "-Bucket-" + partitionBucket;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof PartitionBucket) {
        return partitionBucket == ((PartitionBucket) obj).partitionBucket;
      }
      return false;
    }

    @Override
    public int hashCode() {
      return partitionBucket;
    }
  }

  public static class PartitionBucketResolver
      implements FixedPartitionResolver<PartitionBucket, Object>, Serializable {

    @Override
    public String getName() {
      return getClass().getName();
    }

    @Override
    public String getPartitionName(EntryOperation<PartitionBucket, Object> opDetails,
        Set<String> targetPartitions) {
      return opDetails.getKey().getPartitionName();
    }

    @Override
    public PartitionBucket getRoutingObject(EntryOperation<PartitionBucket, Object> opDetails) {
      return opDetails.getKey();
    }
  }
}
