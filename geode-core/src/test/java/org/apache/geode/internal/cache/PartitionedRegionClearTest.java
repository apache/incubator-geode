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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.PartitionedRegionPartialClearException;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.distributed.internal.DMStats;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.MembershipListener;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.partitioned.RegionAdvisor;
import org.apache.geode.internal.cache.versions.RegionVersionVector;

public class PartitionedRegionClearTest {


  private PartitionedRegionClear partitionedRegionClear;
  private DistributionManager distributionManager;
  private PartitionedRegion partitionedRegion;

  @Before
  public void setUp() {

    partitionedRegion = mock(PartitionedRegion.class);
    distributionManager = mock(DistributionManager.class);

    when(partitionedRegion.getDistributionManager()).thenReturn(distributionManager);
    when(partitionedRegion.getName()).thenReturn("prRegion");

    partitionedRegionClear = new PartitionedRegionClear(partitionedRegion);
  }

  private Set<BucketRegion> setupBucketRegions(
      PartitionedRegionDataStore partitionedRegionDataStore,
      BucketAdvisor bucketAdvisor) {
    final int numBuckets = 2;
    HashSet<BucketRegion> bucketRegions = new HashSet<>();
    for (int i = 0; i < numBuckets; i++) {
      BucketRegion bucketRegion = mock(BucketRegion.class);
      when(bucketRegion.getBucketAdvisor()).thenReturn(bucketAdvisor);
      when(bucketRegion.size()).thenReturn(1);
      when(bucketRegion.getId()).thenReturn(i);
      bucketRegions.add(bucketRegion);
    }

    when(partitionedRegionDataStore.getAllLocalBucketRegions()).thenReturn(bucketRegions);
    when(partitionedRegionDataStore.getAllLocalPrimaryBucketRegions()).thenReturn(bucketRegions);

    return bucketRegions;
  }

  @Test
  public void isLockedForListenerAndClientNotificationReturnsTrueWhenLocked() {
    InternalDistributedMember internalDistributedMember = mock(InternalDistributedMember.class);
    when(distributionManager.isCurrentMember(internalDistributedMember)).thenReturn(true);
    partitionedRegionClear.obtainClearLockLocal(internalDistributedMember);

    assertThat(partitionedRegionClear.isLockedForListenerAndClientNotification()).isTrue();
  }

  @Test
  public void isLockedForListenerAndClientNotificationReturnsFalseWhenMemberNotInTheSystemRequestsLock() {
    InternalDistributedMember internalDistributedMember = mock(InternalDistributedMember.class);
    when(distributionManager.isCurrentMember(internalDistributedMember)).thenReturn(false);

    assertThat(partitionedRegionClear.isLockedForListenerAndClientNotification()).isFalse();
  }

  @Test
  public void acquireDistributedClearLockGetsDistributedLock() {
    DistributedLockService distributedLockService = mock(DistributedLockService.class);
    String lockName = PartitionedRegionClear.CLEAR_OPERATION + partitionedRegion.getName();
    when(partitionedRegion.getPartitionedRegionLockService()).thenReturn(distributedLockService);

    partitionedRegionClear.acquireDistributedClearLock(lockName);

    verify(distributedLockService, times(1)).lock(lockName, -1, -1);
  }

  @Test
  public void releaseDistributedClearLockReleasesDistributedLock() {
    DistributedLockService distributedLockService = mock(DistributedLockService.class);
    String lockName = PartitionedRegionClear.CLEAR_OPERATION + partitionedRegion.getName();
    when(partitionedRegion.getPartitionedRegionLockService()).thenReturn(distributedLockService);

    partitionedRegionClear.releaseDistributedClearLock(lockName);

    verify(distributedLockService, times(1)).unlock(lockName);
  }

  @Test
  public void obtainLockForClearGetsLocalLockAndSendsMessageForRemote() throws Exception {
    RegionEventImpl regionEvent = mock(RegionEventImpl.class);
    when(regionEvent.clone()).thenReturn(mock(RegionEventImpl.class));
    Region<String, PartitionRegionConfig> region = mock(Region.class);
    when(partitionedRegion.getPRRoot()).thenReturn(region);
    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);
    doReturn(Collections.EMPTY_LIST).when(spyPartitionedRegionClear)
        .attemptToSendPartitionedRegionClearMessage(regionEvent,
            PartitionedRegionClearMessage.OperationType.OP_LOCK_FOR_PR_CLEAR);
    InternalDistributedMember internalDistributedMember = mock(InternalDistributedMember.class);
    when(distributionManager.getId()).thenReturn(internalDistributedMember);

    spyPartitionedRegionClear.obtainLockForClear(regionEvent);

    verify(spyPartitionedRegionClear, times(1)).obtainClearLockLocal(internalDistributedMember);
    verify(spyPartitionedRegionClear, times(1)).sendPartitionedRegionClearMessage(regionEvent,
        PartitionedRegionClearMessage.OperationType.OP_LOCK_FOR_PR_CLEAR);
  }

  @Test
  public void releaseLockForClearReleasesLocalLockAndSendsMessageForRemote() throws Exception {
    RegionEventImpl regionEvent = mock(RegionEventImpl.class);
    when(regionEvent.clone()).thenReturn(mock(RegionEventImpl.class));
    Region<String, PartitionRegionConfig> region = mock(Region.class);
    when(partitionedRegion.getPRRoot()).thenReturn(region);
    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);
    doReturn(Collections.EMPTY_LIST).when(spyPartitionedRegionClear)
        .attemptToSendPartitionedRegionClearMessage(regionEvent,
            PartitionedRegionClearMessage.OperationType.OP_UNLOCK_FOR_PR_CLEAR);
    InternalDistributedMember internalDistributedMember = mock(InternalDistributedMember.class);
    when(distributionManager.getId()).thenReturn(internalDistributedMember);

    spyPartitionedRegionClear.releaseLockForClear(regionEvent);

    verify(spyPartitionedRegionClear, times(1)).releaseClearLockLocal();
    verify(spyPartitionedRegionClear, times(1)).sendPartitionedRegionClearMessage(regionEvent,
        PartitionedRegionClearMessage.OperationType.OP_UNLOCK_FOR_PR_CLEAR);
  }

  @Test
  public void clearRegionClearsLocalAndSendsMessageForRemote() throws Exception {
    RegionEventImpl regionEvent = mock(RegionEventImpl.class);
    when(regionEvent.clone()).thenReturn(mock(RegionEventImpl.class));
    Region<String, PartitionRegionConfig> region = mock(Region.class);
    when(partitionedRegion.getPRRoot()).thenReturn(region);
    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);
    doReturn(Collections.EMPTY_LIST).when(spyPartitionedRegionClear)
        .attemptToSendPartitionedRegionClearMessage(regionEvent,
            PartitionedRegionClearMessage.OperationType.OP_PR_CLEAR);
    InternalDistributedMember internalDistributedMember = mock(InternalDistributedMember.class);
    when(distributionManager.getId()).thenReturn(internalDistributedMember);
    RegionVersionVector regionVersionVector = mock(RegionVersionVector.class);

    spyPartitionedRegionClear.clearRegion(regionEvent, false, regionVersionVector);

    verify(spyPartitionedRegionClear, times(1)).clearRegionLocal(regionEvent);
    verify(spyPartitionedRegionClear, times(1)).sendPartitionedRegionClearMessage(regionEvent,
        PartitionedRegionClearMessage.OperationType.OP_PR_CLEAR);
  }

  @Test
  public void waitForPrimaryReturnsAfterFindingAllPrimary() {
    PartitionedRegionDataStore partitionedRegionDataStore = mock(PartitionedRegionDataStore.class);
    BucketAdvisor bucketAdvisor = mock(BucketAdvisor.class);
    when(bucketAdvisor.hasPrimary()).thenReturn(true);
    setupBucketRegions(partitionedRegionDataStore, bucketAdvisor);
    when(partitionedRegion.getDataStore()).thenReturn(partitionedRegionDataStore);
    PartitionedRegion.RetryTimeKeeper retryTimer = mock(PartitionedRegion.RetryTimeKeeper.class);

    partitionedRegionClear.waitForPrimary(retryTimer);

    verify(retryTimer, times(0)).waitForBucketsRecovery();
  }

  @Test
  public void waitForPrimaryReturnsAfterRetryForPrimary() {
    PartitionedRegionDataStore partitionedRegionDataStore = mock(PartitionedRegionDataStore.class);
    BucketAdvisor bucketAdvisor = mock(BucketAdvisor.class);
    when(bucketAdvisor.hasPrimary()).thenReturn(false).thenReturn(true);
    setupBucketRegions(partitionedRegionDataStore, bucketAdvisor);
    when(partitionedRegion.getDataStore()).thenReturn(partitionedRegionDataStore);
    PartitionedRegion.RetryTimeKeeper retryTimer = mock(PartitionedRegion.RetryTimeKeeper.class);

    partitionedRegionClear.waitForPrimary(retryTimer);

    verify(retryTimer, times(1)).waitForBucketsRecovery();
  }

  @Test
  public void waitForPrimaryThrowsPartitionedRegionPartialClearException() {
    PartitionedRegionDataStore partitionedRegionDataStore = mock(PartitionedRegionDataStore.class);
    BucketAdvisor bucketAdvisor = mock(BucketAdvisor.class);
    setupBucketRegions(partitionedRegionDataStore, bucketAdvisor);
    when(partitionedRegion.getDataStore()).thenReturn(partitionedRegionDataStore);
    PartitionedRegion.RetryTimeKeeper retryTimer = mock(PartitionedRegion.RetryTimeKeeper.class);
    when(retryTimer.overMaximum()).thenReturn(true);

    Throwable thrown = catchThrowable(() -> partitionedRegionClear.waitForPrimary(retryTimer));

    assertThat(thrown)
        .isInstanceOf(PartitionedRegionPartialClearException.class)
        .hasMessage(
            "Unable to find primary bucket region during clear operation for region: prRegion");
    verify(retryTimer, times(0)).waitForBucketsRecovery();
  }

  @Test
  public void clearRegionLocalCallsClearOnLocalPrimaryBucketRegions() {
    RegionEventImpl regionEvent = mock(RegionEventImpl.class);
    BucketAdvisor bucketAdvisor = mock(BucketAdvisor.class);
    when(bucketAdvisor.hasPrimary()).thenReturn(true);
    PartitionedRegionDataStore partitionedRegionDataStore = mock(PartitionedRegionDataStore.class);
    doNothing().when(partitionedRegionDataStore).lockBucketCreationForRegionClear();
    Set<BucketRegion> buckets = setupBucketRegions(partitionedRegionDataStore, bucketAdvisor);
    when(partitionedRegion.getDataStore()).thenReturn(partitionedRegionDataStore);

    List bucketsCleared = partitionedRegionClear.clearRegionLocal(regionEvent);

    assertThat(bucketsCleared.size()).isEqualTo(buckets.size());
    for (BucketRegion bucketRegion : buckets) {
      verify(bucketRegion, times(1)).clear();
    }
  }

  @Test
  public void clearRegionLocalRetriesClearOnLocalPrimaryBucketRegions() {
    RegionEventImpl regionEvent = mock(RegionEventImpl.class);
    BucketAdvisor bucketAdvisor = mock(BucketAdvisor.class);
    when(bucketAdvisor.hasPrimary()).thenReturn(true);
    PartitionedRegionDataStore partitionedRegionDataStore = mock(PartitionedRegionDataStore.class);
    doNothing().when(partitionedRegionDataStore).lockBucketCreationForRegionClear();
    Set<BucketRegion> buckets = setupBucketRegions(partitionedRegionDataStore, bucketAdvisor);
    when(partitionedRegion.getDataStore()).thenReturn(partitionedRegionDataStore);
    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);
    when(spyPartitionedRegionClear.getMembershipChange()).thenReturn(true).thenReturn(false);

    List bucketsCleared = spyPartitionedRegionClear.clearRegionLocal(regionEvent);

    int expectedClears = buckets.size() * 2; /* clear is called twice on each bucket */
    assertThat(bucketsCleared.size()).isEqualTo(expectedClears);
    for (BucketRegion bucketRegion : buckets) {
      verify(bucketRegion, times(2)).clear();
    }
  }

  @Test
  public void doAfterClearCallsNotifyClientsWhenClientHaveInterests() {
    RegionEventImpl regionEvent = mock(RegionEventImpl.class);
    when(partitionedRegion.hasAnyClientsInterested()).thenReturn(true);
    FilterProfile filterProfile = mock(FilterProfile.class);
    FilterRoutingInfo filterRoutingInfo = mock(FilterRoutingInfo.class);
    when(filterProfile.getFilterRoutingInfoPart1(regionEvent, FilterProfile.NO_PROFILES,
        Collections.emptySet())).thenReturn(filterRoutingInfo);
    when(filterProfile.getFilterRoutingInfoPart2(filterRoutingInfo, regionEvent)).thenReturn(
        filterRoutingInfo);
    when(partitionedRegion.getFilterProfile()).thenReturn(filterProfile);

    partitionedRegionClear.doAfterClear(regionEvent);

    verify(regionEvent, times(1)).setLocalFilterInfo(any());
    verify(partitionedRegion, times(1)).notifyBridgeClients(regionEvent);
  }

  @Test
  public void doAfterClearDispatchesListenerEvents() {
    RegionEventImpl regionEvent = mock(RegionEventImpl.class);
    when(partitionedRegion.hasListener()).thenReturn(true);

    partitionedRegionClear.doAfterClear(regionEvent);

    verify(partitionedRegion, times(1)).dispatchListenerEvent(
        EnumListenerEvent.AFTER_REGION_CLEAR, regionEvent);
  }

  @Test
  public void obtainClearLockLocalGetsLockOnPrimaryBuckets() {
    BucketAdvisor bucketAdvisor = mock(BucketAdvisor.class);
    when(bucketAdvisor.hasPrimary()).thenReturn(true);
    PartitionedRegionDataStore partitionedRegionDataStore = mock(PartitionedRegionDataStore.class);
    Set<BucketRegion> buckets = setupBucketRegions(partitionedRegionDataStore, bucketAdvisor);
    when(partitionedRegion.getDataStore()).thenReturn(partitionedRegionDataStore);
    InternalDistributedMember member = mock(InternalDistributedMember.class);
    when(distributionManager.isCurrentMember(member)).thenReturn(true);

    partitionedRegionClear.obtainClearLockLocal(member);

    assertThat(partitionedRegionClear.lockForListenerAndClientNotification.getLockRequester())
        .isSameAs(member);
    for (BucketRegion bucketRegion : buckets) {
      verify(bucketRegion, times(1)).lockLocallyForClear(partitionedRegion.getDistributionManager(),
          partitionedRegion.getMyId(), null);
    }
  }

  @Test
  public void obtainClearLockLocalDoesNotGetLocksOnPrimaryBucketsWhenMemberIsNotCurrent() {
    BucketAdvisor bucketAdvisor = mock(BucketAdvisor.class);
    when(bucketAdvisor.hasPrimary()).thenReturn(true);
    PartitionedRegionDataStore partitionedRegionDataStore = mock(PartitionedRegionDataStore.class);
    Set<BucketRegion> buckets = setupBucketRegions(partitionedRegionDataStore, bucketAdvisor);
    when(partitionedRegion.getDataStore()).thenReturn(partitionedRegionDataStore);
    InternalDistributedMember member = mock(InternalDistributedMember.class);
    when(distributionManager.isCurrentMember(member)).thenReturn(false);

    partitionedRegionClear.obtainClearLockLocal(member);

    assertThat(partitionedRegionClear.lockForListenerAndClientNotification.getLockRequester())
        .isNull();
    for (BucketRegion bucketRegion : buckets) {
      verify(bucketRegion, times(0)).lockLocallyForClear(partitionedRegion.getDistributionManager(),
          partitionedRegion.getMyId(), null);
    }
  }

  @Test
  public void releaseClearLockLocalReleasesLockOnPrimaryBuckets() {
    BucketAdvisor bucketAdvisor = mock(BucketAdvisor.class);
    when(bucketAdvisor.hasPrimary()).thenReturn(true);
    PartitionedRegionDataStore partitionedRegionDataStore = mock(PartitionedRegionDataStore.class);
    Set<BucketRegion> buckets = setupBucketRegions(partitionedRegionDataStore, bucketAdvisor);
    when(partitionedRegion.getDataStore()).thenReturn(partitionedRegionDataStore);
    InternalDistributedMember member = mock(InternalDistributedMember.class);
    when(distributionManager.isCurrentMember(member)).thenReturn(true);
    partitionedRegionClear.lockForListenerAndClientNotification.setLocked(member);

    partitionedRegionClear.releaseClearLockLocal();

    for (BucketRegion bucketRegion : buckets) {
      verify(bucketRegion, times(1)).releaseLockLocallyForClear(null);
    }
  }

  @Test
  public void releaseClearLockLocalDoesNotReleaseLocksOnPrimaryBucketsWhenMemberIsNotCurrent() {
    BucketAdvisor bucketAdvisor = mock(BucketAdvisor.class);
    when(bucketAdvisor.hasPrimary()).thenReturn(true);
    PartitionedRegionDataStore partitionedRegionDataStore = mock(PartitionedRegionDataStore.class);
    Set<BucketRegion> buckets = setupBucketRegions(partitionedRegionDataStore, bucketAdvisor);
    when(partitionedRegion.getDataStore()).thenReturn(partitionedRegionDataStore);
    InternalDistributedMember member = mock(InternalDistributedMember.class);

    partitionedRegionClear.obtainClearLockLocal(member);

    assertThat(partitionedRegionClear.lockForListenerAndClientNotification.getLockRequester())
        .isNull();
    for (BucketRegion bucketRegion : buckets) {
      verify(bucketRegion, times(0)).releaseLockLocallyForClear(null);
    }
  }

  @Test
  public void sendPartitionedRegionClearMessageSendsClearMessageToPRNodes() {
    RegionEventImpl regionEvent = mock(RegionEventImpl.class);
    when(regionEvent.clone()).thenReturn(mock(RegionEventImpl.class));
    Region<String, PartitionRegionConfig> prRoot = mock(Region.class);
    when(partitionedRegion.getPRRoot()).thenReturn(prRoot);
    InternalDistributedMember member = mock(InternalDistributedMember.class);
    RegionAdvisor regionAdvisor = mock(RegionAdvisor.class);
    Set<InternalDistributedMember> prNodes = new HashSet<>();
    prNodes.add(member);
    Node node = mock(Node.class);
    when(node.getMemberId()).thenReturn(member);
    Set<Node> configNodes = new HashSet<>();
    configNodes.add(node);
    when(regionAdvisor.adviseAllPRNodes()).thenReturn(prNodes);
    when(partitionedRegion.getRegionAdvisor()).thenReturn(regionAdvisor);
    PartitionRegionConfig partitionRegionConfig = mock(PartitionRegionConfig.class);
    when(partitionRegionConfig.getNodes()).thenReturn(configNodes);
    when(prRoot.get(any())).thenReturn(partitionRegionConfig);
    InternalDistributedSystem internalDistributedSystem = mock(InternalDistributedSystem.class);
    when(internalDistributedSystem.getDistributionManager()).thenReturn(distributionManager);
    when(partitionedRegion.getSystem()).thenReturn(internalDistributedSystem);
    when(partitionedRegion.isTransactionDistributed()).thenReturn(false);
    when(distributionManager.getCancelCriterion()).thenReturn(mock(CancelCriterion.class));
    when(distributionManager.getStats()).thenReturn(mock(DMStats.class));

    partitionedRegionClear.sendPartitionedRegionClearMessage(regionEvent,
        PartitionedRegionClearMessage.OperationType.OP_PR_CLEAR);

    verify(distributionManager, times(1)).putOutgoing(any());
  }

  @Test
  public void doClearAcquiresAndReleasesDistributedClearLockAndCreatesAllPrimaryBuckets() {
    RegionEventImpl regionEvent = mock(RegionEventImpl.class);
    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);
    doNothing().when(spyPartitionedRegionClear).acquireDistributedClearLock(any());
    doNothing().when(spyPartitionedRegionClear).assignAllPrimaryBuckets();
    doReturn(Collections.EMPTY_LIST).when(spyPartitionedRegionClear).clearRegion(regionEvent, false,
        null);

    spyPartitionedRegionClear.doClear(regionEvent, false);

    verify(spyPartitionedRegionClear, times(1)).acquireDistributedClearLock(any());
    verify(spyPartitionedRegionClear, times(1)).releaseDistributedClearLock(any());
    verify(spyPartitionedRegionClear, times(1)).assignAllPrimaryBuckets();
  }

  @Test
  public void doClearInvokesCacheWriterWhenCacheWriteIsSet() {
    boolean cacheWrite = true;
    RegionEventImpl regionEvent = mock(RegionEventImpl.class);
    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);
    doNothing().when(spyPartitionedRegionClear).acquireDistributedClearLock(any());
    doNothing().when(spyPartitionedRegionClear).assignAllPrimaryBuckets();
    doReturn(Collections.EMPTY_LIST).when(spyPartitionedRegionClear).clearRegion(regionEvent,
        cacheWrite, null);

    spyPartitionedRegionClear.doClear(regionEvent, cacheWrite);

    verify(spyPartitionedRegionClear, times(1)).invokeCacheWriter(regionEvent);
  }

  @Test
  public void doClearDoesNotInvokesCacheWriterWhenCacheWriteIsNotSet() {
    boolean cacheWrite = false;
    RegionEventImpl regionEvent = mock(RegionEventImpl.class);
    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);
    doNothing().when(spyPartitionedRegionClear).acquireDistributedClearLock(any());
    doNothing().when(spyPartitionedRegionClear).assignAllPrimaryBuckets();
    doReturn(Collections.EMPTY_LIST).when(spyPartitionedRegionClear).clearRegion(regionEvent,
        cacheWrite, null);

    spyPartitionedRegionClear.doClear(regionEvent, cacheWrite);

    verify(spyPartitionedRegionClear, times(0)).invokeCacheWriter(regionEvent);
  }

  @Test
  public void doClearObtainsAndReleasesLockForClearWhenRegionHasListener() {
    boolean cacheWrite = false;
    RegionEventImpl regionEvent = mock(RegionEventImpl.class);
    when(partitionedRegion.hasListener()).thenReturn(true);
    when(partitionedRegion.hasAnyClientsInterested()).thenReturn(false);
    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);
    doNothing().when(spyPartitionedRegionClear).acquireDistributedClearLock(any());
    doNothing().when(spyPartitionedRegionClear).assignAllPrimaryBuckets();
    doNothing().when(spyPartitionedRegionClear).obtainLockForClear(regionEvent);
    doNothing().when(spyPartitionedRegionClear).releaseLockForClear(regionEvent);
    doReturn(Collections.EMPTY_LIST).when(spyPartitionedRegionClear).clearRegion(regionEvent,
        cacheWrite, null);

    spyPartitionedRegionClear.doClear(regionEvent, cacheWrite);

    verify(spyPartitionedRegionClear, times(1)).obtainLockForClear(regionEvent);
    verify(spyPartitionedRegionClear, times(1)).releaseLockForClear(regionEvent);
  }

  @Test
  public void doClearObtainsAndReleasesLockForClearWhenRegionHasClientInterest() {
    boolean cacheWrite = false;
    RegionEventImpl regionEvent = mock(RegionEventImpl.class);
    when(partitionedRegion.hasListener()).thenReturn(false);
    when(partitionedRegion.hasAnyClientsInterested()).thenReturn(true);
    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);
    doNothing().when(spyPartitionedRegionClear).acquireDistributedClearLock(any());
    doNothing().when(spyPartitionedRegionClear).assignAllPrimaryBuckets();
    doNothing().when(spyPartitionedRegionClear).obtainLockForClear(regionEvent);
    doNothing().when(spyPartitionedRegionClear).releaseLockForClear(regionEvent);
    doReturn(Collections.EMPTY_LIST).when(spyPartitionedRegionClear).clearRegion(regionEvent,
        cacheWrite, null);

    spyPartitionedRegionClear.doClear(regionEvent, cacheWrite);

    verify(spyPartitionedRegionClear, times(1)).obtainLockForClear(regionEvent);
    verify(spyPartitionedRegionClear, times(1)).releaseLockForClear(regionEvent);
  }

  @Test
  public void doClearDoesNotObtainLockForClearWhenRegionHasNoListenerAndNoClientInterest() {
    boolean cacheWrite = false;
    RegionEventImpl regionEvent = mock(RegionEventImpl.class);
    when(partitionedRegion.hasListener()).thenReturn(false);
    when(partitionedRegion.hasAnyClientsInterested()).thenReturn(false);
    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);
    doNothing().when(spyPartitionedRegionClear).acquireDistributedClearLock(any());
    doNothing().when(spyPartitionedRegionClear).assignAllPrimaryBuckets();
    doNothing().when(spyPartitionedRegionClear).obtainLockForClear(regionEvent);
    doNothing().when(spyPartitionedRegionClear).releaseLockForClear(regionEvent);
    doReturn(Collections.EMPTY_LIST).when(spyPartitionedRegionClear).clearRegion(regionEvent,
        cacheWrite, null);

    spyPartitionedRegionClear.doClear(regionEvent, cacheWrite);

    verify(spyPartitionedRegionClear, times(0)).obtainLockForClear(regionEvent);
    verify(spyPartitionedRegionClear, times(0)).releaseLockForClear(regionEvent);
  }

  @Test
  public void doClearThrowsPartitionedRegionPartialClearException() {
    boolean cacheWrite = false;
    RegionEventImpl regionEvent = mock(RegionEventImpl.class);
    when(partitionedRegion.hasListener()).thenReturn(false);
    when(partitionedRegion.hasAnyClientsInterested()).thenReturn(false);
    when(partitionedRegion.getTotalNumberOfBuckets()).thenReturn(1);
    when(partitionedRegion.getName()).thenReturn("prRegion");
    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);
    doNothing().when(spyPartitionedRegionClear).acquireDistributedClearLock(any());
    doNothing().when(spyPartitionedRegionClear).assignAllPrimaryBuckets();
    doNothing().when(spyPartitionedRegionClear).obtainLockForClear(regionEvent);
    doNothing().when(spyPartitionedRegionClear).releaseLockForClear(regionEvent);
    doReturn(Collections.EMPTY_LIST).when(spyPartitionedRegionClear).clearRegion(regionEvent,
        cacheWrite, null);

    Throwable thrown =
        catchThrowable(() -> spyPartitionedRegionClear.doClear(regionEvent, cacheWrite));

    assertThat(thrown)
        .isInstanceOf(PartitionedRegionPartialClearException.class)
        .hasMessage(
            "Unable to clear all the buckets from the partitioned region prRegion, either data (buckets) moved or member departed.");
  }

  @Test
  public void handleClearFromDepartedMemberReleasesTheLockForRequesterDeparture() {
    InternalDistributedMember member = mock(InternalDistributedMember.class);
    partitionedRegionClear.lockForListenerAndClientNotification.setLocked(member);
    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);

    spyPartitionedRegionClear.handleClearFromDepartedMember(member);

    verify(spyPartitionedRegionClear, times(1)).releaseClearLockLocal();
  }

  @Test
  public void handleClearFromDepartedMemberDoesNotReleasesTheLockForNonRequesterDeparture() {
    InternalDistributedMember requesterMember = mock(InternalDistributedMember.class);
    InternalDistributedMember member = mock(InternalDistributedMember.class);
    partitionedRegionClear.lockForListenerAndClientNotification.setLocked(requesterMember);
    PartitionedRegionClear spyPartitionedRegionClear = spy(partitionedRegionClear);

    spyPartitionedRegionClear.handleClearFromDepartedMember(member);

    verify(spyPartitionedRegionClear, times(0)).releaseClearLockLocal();
  }

  @Test
  public void partitionedRegionClearRegistersMembershipListener() {
    MembershipListener membershipListener =
        partitionedRegionClear.getPartitionedRegionClearListener();

    verify(distributionManager, times(1)).addMembershipListener(membershipListener);
  }

  @Test
  public void lockRequesterDepartureReleasesTheLock() {
    InternalDistributedMember member = mock(InternalDistributedMember.class);
    partitionedRegionClear.lockForListenerAndClientNotification.setLocked(member);
    PartitionedRegionClear.PartitionedRegionClearListener partitionedRegionClearListener =
        partitionedRegionClear.getPartitionedRegionClearListener();

    partitionedRegionClearListener.memberDeparted(distributionManager, member, true);

    assertThat(partitionedRegionClear.getMembershipChange()).isTrue();
    assertThat(partitionedRegionClear.lockForListenerAndClientNotification.getLockRequester())
        .isNull();
  }

  @Test
  public void nonLockRequesterDepartureDoesNotReleasesTheLock() {
    InternalDistributedMember requesterMember = mock(InternalDistributedMember.class);
    InternalDistributedMember member = mock(InternalDistributedMember.class);
    partitionedRegionClear.lockForListenerAndClientNotification.setLocked(requesterMember);
    PartitionedRegionClear.PartitionedRegionClearListener partitionedRegionClearListener =
        partitionedRegionClear.getPartitionedRegionClearListener();

    partitionedRegionClearListener.memberDeparted(distributionManager, member, true);

    assertThat(partitionedRegionClear.getMembershipChange()).isTrue();
    assertThat(partitionedRegionClear.lockForListenerAndClientNotification.getLockRequester())
        .isNotNull();
  }
}
