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
package org.apache.geode.internal.cache;

import static org.apache.geode.internal.cache.CacheServerImpl.CACHE_SERVER_BIND_ADDRESS_NOT_AVAILABLE_EXCEPTION_MESSAGE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.mockito.Mockito;

import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.partitioned.Bucket;
import org.apache.geode.internal.cache.partitioned.RegionAdvisor;

public class BucketAdvisorTest {

  @Test
  public void shouldBeMockable() throws Exception {
    BucketAdvisor mockBucketAdvisor = mock(BucketAdvisor.class);
    InternalDistributedMember mockInternalDistributedMember = mock(InternalDistributedMember.class);

    when(mockBucketAdvisor.basicGetPrimaryMember()).thenReturn(mockInternalDistributedMember);
    when(mockBucketAdvisor.getBucketRedundancy()).thenReturn(1);

    assertThat(mockBucketAdvisor.basicGetPrimaryMember()).isEqualTo(mockInternalDistributedMember);
    assertThat(mockBucketAdvisor.getBucketRedundancy()).isEqualTo(1);
  }

  @Test
  public void whenServerStopsAfterTheFirstIsRunningCheckThenItShouldNotBeAddedToLocations() {
    InternalCache mockCache = mock(InternalCache.class);
    ProxyBucketRegion mockBucket = mock(ProxyBucketRegion.class);
    RegionAdvisor mockRegionAdvisor = mock(RegionAdvisor.class);
    PartitionedRegion mockPartitionedRegion = mock(PartitionedRegion.class);
    @SuppressWarnings("rawtypes")
    PartitionAttributes mockPartitionAttributes = mock(PartitionAttributes.class);
    DistributionManager mockDistributionManager = mock(DistributionManager.class);
    List<CacheServer> cacheServers = new ArrayList<>();
    CacheServerImpl mockCacheServer = mock(CacheServerImpl.class);
    cacheServers.add(mockCacheServer);

    when(mockRegionAdvisor.getPartitionedRegion()).thenReturn(mockPartitionedRegion);
    when(mockPartitionedRegion.getPartitionAttributes()).thenReturn(mockPartitionAttributes);
    when(mockBucket.getCache()).thenReturn(mockCache);
    when(mockCache.getCacheServers()).thenReturn(cacheServers);
    when(mockPartitionAttributes.getColocatedWith()).thenReturn(null);
    when(mockBucket.getDistributionManager()).thenReturn(mockDistributionManager);
    doNothing().when(mockDistributionManager).addMembershipListener(any());
    when(mockCacheServer.isRunning()).thenReturn(true);
    when(mockCacheServer.getExternalAddress()).thenThrow(
        new IllegalStateException(CACHE_SERVER_BIND_ADDRESS_NOT_AVAILABLE_EXCEPTION_MESSAGE));

    BucketAdvisor bucketAdvisor = BucketAdvisor.createBucketAdvisor(mockBucket, mockRegionAdvisor);
    assertThat(bucketAdvisor.getBucketServerLocations(0).size()).isEqualTo(0);
  }

  @Test
  public void whenServerThrowsIllegalStateExceptionWithoutBindAddressMsgThenExceptionMustBeThrown() {
    InternalCache mockCache = mock(InternalCache.class);
    ProxyBucketRegion mockBucket = mock(ProxyBucketRegion.class);
    RegionAdvisor mockRegionAdvisor = mock(RegionAdvisor.class);
    PartitionedRegion mockPartitionedRegion = mock(PartitionedRegion.class);
    @SuppressWarnings("rawtypes")
    PartitionAttributes mockPartitionAttributes = mock(PartitionAttributes.class);
    DistributionManager mockDistributionManager = mock(DistributionManager.class);
    List<CacheServer> cacheServers = new ArrayList<>();
    CacheServerImpl mockCacheServer = mock(CacheServerImpl.class);
    cacheServers.add(mockCacheServer);

    when(mockRegionAdvisor.getPartitionedRegion()).thenReturn(mockPartitionedRegion);
    when(mockPartitionedRegion.getPartitionAttributes()).thenReturn(mockPartitionAttributes);
    when(mockBucket.getCache()).thenReturn(mockCache);
    when(mockCache.getCacheServers()).thenReturn(cacheServers);
    when(mockPartitionAttributes.getColocatedWith()).thenReturn(null);
    when(mockBucket.getDistributionManager()).thenReturn(mockDistributionManager);
    doNothing().when(mockDistributionManager).addMembershipListener(any());
    when(mockCacheServer.isRunning()).thenReturn(true);
    when(mockCacheServer.getExternalAddress()).thenThrow(new IllegalStateException());

    BucketAdvisor bucketAdvisor = BucketAdvisor.createBucketAdvisor(mockBucket, mockRegionAdvisor);
    assertThatThrownBy(() -> bucketAdvisor.getBucketServerLocations(0))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void volunteerForPrimaryIgnoresMissingPrimaryElector() {
    DistributionManager distributionManager = mock(DistributionManager.class);
    when(distributionManager.getId()).thenReturn(new InternalDistributedMember("localhost", 321));

    Bucket bucket = mock(Bucket.class);
    when(bucket.isHosting()).thenReturn(true);
    when(bucket.isPrimary()).thenReturn(false);
    when(bucket.getDistributionManager()).thenReturn(distributionManager);

    PartitionedRegion partitionedRegion = mock(PartitionedRegion.class);
    when(partitionedRegion.getRedundantCopies()).thenReturn(0);
    when(partitionedRegion.getPartitionAttributes()).thenReturn(new PartitionAttributesImpl());
    when(partitionedRegion.getRedundancyTracker())
        .thenReturn(mock(PartitionedRegionRedundancyTracker.class));

    InternalDistributedMember missingElectorId = new InternalDistributedMember("localhost", 123);

    RegionAdvisor regionAdvisor = mock(RegionAdvisor.class);
    when(regionAdvisor.getPartitionedRegion()).thenReturn(partitionedRegion);
    // hasPartitionedRegion() is invoked twice - once in initializePrimaryElector() and then in
    // volunteerForPrimary(). Returning true first simulates a elector being
    // there when createBucketAtomically() initiates creation of a bucket. Returning
    // false the second time simulates the elector closing its region/cache before
    // we get to the point of volunteering for primary
    when(regionAdvisor.hasPartitionedRegion(Mockito.any(InternalDistributedMember.class)))
        .thenReturn(true,
            false);

    BucketAdvisor advisor = BucketAdvisor.createBucketAdvisor(bucket, regionAdvisor);
    BucketAdvisor advisorSpy = spy(advisor);
    doCallRealMethod().when(advisorSpy).exchangeProfiles();
    doCallRealMethod().when(advisorSpy).volunteerForPrimary();
    doReturn(true).when(advisorSpy).initializationGate();
    doReturn(true).when(advisorSpy).isHosting();

    BucketAdvisor.VolunteeringDelegate volunteeringDelegate =
        mock(BucketAdvisor.VolunteeringDelegate.class);
    advisorSpy.setVolunteeringDelegate(volunteeringDelegate);
    advisorSpy.initializePrimaryElector(missingElectorId);
    assertEquals(missingElectorId, advisorSpy.getPrimaryElector());
    advisorSpy.volunteerForPrimary();
    verify(volunteeringDelegate).volunteerForPrimary();
  }

  @Test
  public void shadowBucketsDestroyedTrackingShouldWorkCorrectly() {
    DistributionManager distributionManager = mock(DistributionManager.class);
    when(distributionManager.getId()).thenReturn(new InternalDistributedMember("localhost", 321));

    Bucket bucket = mock(Bucket.class);
    when(bucket.isHosting()).thenReturn(true);
    when(bucket.isPrimary()).thenReturn(false);
    when(bucket.getDistributionManager()).thenReturn(distributionManager);

    PartitionedRegion partitionedRegion = mock(PartitionedRegion.class);
    when(partitionedRegion.getRedundantCopies()).thenReturn(0);
    when(partitionedRegion.getPartitionAttributes()).thenReturn(new PartitionAttributesImpl());
    RegionAdvisor regionAdvisor = mock(RegionAdvisor.class);
    when(regionAdvisor.getPartitionedRegion()).thenReturn(partitionedRegion);

    List<String> shadowBuckets = Arrays.asList("/bucket1", "/bucket2", "/bucket3");
    BucketAdvisor bucketAdvisor = BucketAdvisor.createBucketAdvisor(bucket, regionAdvisor);
    shadowBuckets.forEach(bucketAdvisor::markShadowBucketAsDestroyed);

    // Return false by default.
    assertThat(bucketAdvisor.isShadowBucketDestroyed("/bucket")).isFalse();

    // Return correct value when found.
    bucketAdvisor.markShadowBucketAsDestroyed(shadowBuckets.get(1));
    assertThat(bucketAdvisor.isShadowBucketDestroyed(shadowBuckets.get(1))).isTrue();

    // Mark all shadow buckets values as non destroyed
    bucketAdvisor.markAllShadowBucketsAsNotDestroyed();
    shadowBuckets
        .forEach(b -> assertThat(assertThat(bucketAdvisor.isShadowBucketDestroyed(b)).isFalse()));

    // Mark all shadow buckets values as destroyed
    bucketAdvisor.markAllShadowBucketsAsDestroyed();
    shadowBuckets
        .forEach(b -> assertThat(assertThat(bucketAdvisor.isShadowBucketDestroyed(b)).isTrue()));
  }
}
