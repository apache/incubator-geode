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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Operation;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.TXCommitMessage.RegionCommit;

public class RegionCommitTest {

  private ClusterDistributionManager dm;
  private String path;
  private LocalRegion region;
  private TXCommitMessage txCommitMessage;
  private RegionCommit regionCommit;
  private final Object key = new Object();
  private RegionCommit.FarSideEntryOp entryOp;

  @Before
  public void setUp() {
    path = "path";

    InternalCache cache = mock(InternalCache.class);

    dm = mock(ClusterDistributionManager.class);
    region = mock(LocalRegion.class);
    txCommitMessage = mock(TXCommitMessage.class);
    regionCommit = new RegionCommit(txCommitMessage);
    entryOp = regionCommit.new FarSideEntryOp();
    entryOp.setOp(Operation.DESTROY);
    entryOp.setKey(key);

    when(dm.getCache()).thenReturn(cache);
    when(cache.getRegionByPath(path)).thenReturn(region);
    when(dm.getSystem()).thenReturn(mock(InternalDistributedSystem.class));
  }

  @Test
  public void getsRegionFromCacheFromDM() {
    assertThat(regionCommit.getRegionByPath(dm, path)).isEqualTo(region);
  }

  @Test
  public void isOpDestroyedEventReturnsFalseIfNotDestroyOperation() {
    entryOp.setOp(Operation.UPDATE);

    assertThat(regionCommit.isOpDestroyEvent(mock(InternalRegion.class), entryOp)).isFalse();
  }

  @Test
  public void isOpDestroyedEventReturnsFalseIfIsDestroyOperationAndRegionEntryToBeDestroyedIsNull() {
    when(region.basicGetEntry(key)).thenReturn(null);

    assertThat(regionCommit.isOpDestroyEvent(region, entryOp)).isFalse();
  }

  @Test
  public void isOpDestroyedEventReturnsFalseIfIsDestroyOperationAndRegionEntryToBeDestroyedIsRemovedToken() {
    RegionEntry regionEntry = mock(RegionEntry.class);

    assertThat(regionCommit.isOpDestroyEvent(region, entryOp)).isFalse();
  }

  @Test
  public void isOpDestroyedEventReturnsFalseIfIsDestroyOperationAndRegionEntryToBeDestroyedIsTombstone() {
    RegionEntry regionEntry = mock(RegionEntry.class);
    when(region.basicGetEntry(key)).thenReturn(regionEntry);
    when(regionEntry.getValue()).thenReturn(Token.TOMBSTONE);

    assertThat(regionCommit.isOpDestroyEvent(region, entryOp)).isFalse();
  }

  @Test
  public void isOpDestroyedEventReturnsTrueIfDestroyEntryOnEmptyRegion() {
    when(region.isProxy()).thenReturn(true);

    assertThat(regionCommit.isOpDestroyEvent(region, entryOp)).isTrue();
  }

  @Test
  public void isOpDestroyedEventReturnsTrueIfIsDestroyOperationAndRegionEntryIsNotAToken() {
    RegionEntry regionEntry = mock(RegionEntry.class);
    when(region.basicGetEntry(key)).thenReturn(regionEntry);
    when(regionEntry.getValue()).thenReturn(new Token.NotAToken());

    assertThat(regionCommit.isOpDestroyEvent(region, entryOp)).isTrue();
  }
}
