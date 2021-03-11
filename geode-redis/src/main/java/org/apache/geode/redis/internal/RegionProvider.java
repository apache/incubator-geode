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
package org.apache.geode.redis.internal;

import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegionFactory;
import org.apache.geode.management.ManagementException;
import org.apache.geode.redis.internal.data.ByteArrayWrapper;
import org.apache.geode.redis.internal.data.RedisData;
import org.apache.geode.redis.internal.executor.cluster.RedisPartitionResolver;

public class RegionProvider {
  /**
   * The name of the region that holds data stored in redis.
   */
  public static final String REDIS_DATA_REGION = "__REDIS_DATA";
  public static final String REDIS_CONFIG_REGION = "__REDIS_CONFIG";
  public static final int REDIS_REGION_BUCKETS = Integer.getInteger("redis.region.buckets", 128);
  public static final int REDIS_SLOTS = Integer.getInteger("redis.slots", 16384);
  public static final int REDIS_SLOTS_PER_BUCKET = REDIS_SLOTS / REDIS_REGION_BUCKETS;

  private final Region<ByteArrayWrapper, RedisData> dataRegion;
  private final Region<String, Object> configRegion;

  public RegionProvider(InternalCache cache) {
    validateBuckets(REDIS_REGION_BUCKETS);

    InternalRegionFactory<ByteArrayWrapper, RedisData> redisDataRegionFactory =
        cache.createInternalRegionFactory(RegionShortcut.PARTITION_REDUNDANT);
    redisDataRegionFactory.setInternalRegion(true).setIsUsedForMetaRegion(true);

    PartitionAttributesFactory<ByteArrayWrapper, RedisData> attributesFactory =
        new PartitionAttributesFactory<>();
    attributesFactory.setPartitionResolver(new RedisPartitionResolver());
    attributesFactory.setTotalNumBuckets(REDIS_REGION_BUCKETS);
    redisDataRegionFactory.setPartitionAttributes(attributesFactory.create());

    dataRegion = redisDataRegionFactory.create(REDIS_DATA_REGION);

    InternalRegionFactory<String, Object> redisConfigRegionFactory =
        cache.createInternalRegionFactory(RegionShortcut.REPLICATE);
    redisConfigRegionFactory.setInternalRegion(true).setIsUsedForMetaRegion(true);
    configRegion = redisConfigRegionFactory.create(REDIS_CONFIG_REGION);
  }

  public Region<ByteArrayWrapper, RedisData> getDataRegion() {
    return dataRegion;
  }

  public Region<String, Object> getConfigRegion() {
    return configRegion;
  }

  /**
   * Validates that the value passed in is a power of 2 and that it is not greater than
   * {@link #REDIS_SLOTS}
   *
   * @throws ManagementException if there is a problem with the value
   */
  protected static void validateBuckets(int buckets) {
    if (buckets <= 0 || ((buckets & (buckets - 1)) != 0)) {
      throw new ManagementException(
          "Could not start Redis Server - redis region buckets must be a power of 2. Configured value is invalid: "
              + buckets);
    }

    if (buckets > REDIS_SLOTS) {
      throw new ManagementException(
          "Could not start Redis Server - redis region buckets must <= " + REDIS_SLOTS);
    }
  }
}
