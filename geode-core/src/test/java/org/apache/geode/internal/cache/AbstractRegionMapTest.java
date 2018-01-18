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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.Operation;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.RegionMap.Attributes;
import org.apache.geode.internal.cache.eviction.EvictionController;
import org.apache.geode.internal.cache.eviction.EvictionCounters;
import org.apache.geode.internal.cache.eviction.EvictionList;
import org.apache.geode.internal.cache.versions.RegionVersionVector;
import org.apache.geode.internal.cache.versions.VersionHolder;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class AbstractRegionMapTest {

  private static final Object KEY = "key";

  @Test
  public void shouldBeMockable() throws Exception {
    AbstractRegionMap mockAbstractRegionMap = mock(AbstractRegionMap.class);
    RegionEntry mockRegionEntry = mock(RegionEntry.class);
    VersionHolder mockVersionHolder = mock(VersionHolder.class);

    when(mockAbstractRegionMap.removeTombstone(eq(mockRegionEntry), eq(mockVersionHolder),
        anyBoolean(), anyBoolean())).thenReturn(true);

    assertThat(
        mockAbstractRegionMap.removeTombstone(mockRegionEntry, mockVersionHolder, true, true))
            .isTrue();
  }

  @Test
  public void invalidateOfNonExistentRegionThrowsEntryNotFound() {
    TestableAbstractRegionMap arm = new TestableAbstractRegionMap();
    EntryEventImpl event = createEventForInvalidate(arm._getOwner());
    when(arm._getOwner().isInitialized()).thenReturn(true);

    assertThatThrownBy(() -> arm.invalidate(event, true, false, false))
        .isInstanceOf(EntryNotFoundException.class);
    verify(arm._getOwner(), never()).basicInvalidatePart2(any(), any(), anyBoolean(), anyBoolean());
    verify(arm._getOwner(), never()).invokeInvalidateCallbacks(any(), any(), anyBoolean());
  }

  @Test
  public void invalidateOfNonExistentRegionThrowsEntryNotFoundWithForce() {
    AbstractRegionMap.FORCE_INVALIDATE_EVENT = true;
    try {
      TestableAbstractRegionMap arm = new TestableAbstractRegionMap();
      EntryEventImpl event = createEventForInvalidate(arm._getOwner());
      when(arm._getOwner().isInitialized()).thenReturn(true);

      assertThatThrownBy(() -> arm.invalidate(event, true, false, false))
          .isInstanceOf(EntryNotFoundException.class);
      verify(arm._getOwner(), never()).basicInvalidatePart2(any(), any(), anyBoolean(),
          anyBoolean());
      verify(arm._getOwner(), times(1)).invokeInvalidateCallbacks(any(), any(), anyBoolean());
    } finally {
      AbstractRegionMap.FORCE_INVALIDATE_EVENT = false;
    }
  }

  @Test
  public void invalidateOfAlreadyInvalidEntryReturnsFalse() {
    TestableAbstractRegionMap arm = new TestableAbstractRegionMap();
    EntryEventImpl event = createEventForInvalidate(arm._getOwner());

    // invalidate on region that is not initialized should create
    // entry in map as invalid.
    assertThatThrownBy(() -> arm.invalidate(event, true, false, false))
        .isInstanceOf(EntryNotFoundException.class);

    when(arm._getOwner().isInitialized()).thenReturn(true);
    assertFalse(arm.invalidate(event, true, false, false));
    verify(arm._getOwner(), never()).basicInvalidatePart2(any(), any(), anyBoolean(), anyBoolean());
    verify(arm._getOwner(), never()).invokeInvalidateCallbacks(any(), any(), anyBoolean());
  }

  @Test
  public void invalidateOfAlreadyInvalidEntryReturnsFalseWithForce() {
    AbstractRegionMap.FORCE_INVALIDATE_EVENT = true;
    try {
      TestableAbstractRegionMap arm = new TestableAbstractRegionMap();
      EntryEventImpl event = createEventForInvalidate(arm._getOwner());

      // invalidate on region that is not initialized should create
      // entry in map as invalid.
      assertThatThrownBy(() -> arm.invalidate(event, true, false, false))
          .isInstanceOf(EntryNotFoundException.class);

      when(arm._getOwner().isInitialized()).thenReturn(true);
      assertFalse(arm.invalidate(event, true, false, false));
      verify(arm._getOwner(), never()).basicInvalidatePart2(any(), any(), anyBoolean(),
          anyBoolean());
      verify(arm._getOwner(), times(1)).invokeInvalidateCallbacks(any(), any(), anyBoolean());
    } finally {
      AbstractRegionMap.FORCE_INVALIDATE_EVENT = false;
    }
  }

  private EntryEventImpl createEventForInvalidate(LocalRegion lr) {
    when(lr.getKeyInfo(KEY)).thenReturn(new KeyInfo(KEY, null, null));
    return EntryEventImpl.create(lr, Operation.INVALIDATE, KEY, false, null, true, false);
  }

  private EntryEventImpl createEventForDestroy(LocalRegion lr) {
    when(lr.getKeyInfo(KEY)).thenReturn(new KeyInfo(KEY, null, null));
    return EntryEventImpl.create(lr, Operation.DESTROY, KEY, false, null, true, false);
  }

  @Test
  public void invalidateForceNewEntryOfAlreadyInvalidEntryReturnsFalse() {
    TestableAbstractRegionMap arm = new TestableAbstractRegionMap();
    EntryEventImpl event = createEventForInvalidate(arm._getOwner());

    // invalidate on region that is not initialized should create
    // entry in map as invalid.
    assertTrue(arm.invalidate(event, true, true, false));
    verify(arm._getOwner(), times(1)).basicInvalidatePart2(any(), any(), anyBoolean(),
        anyBoolean());

    when(arm._getOwner().isInitialized()).thenReturn(true);
    assertFalse(arm.invalidate(event, true, true, false));
    verify(arm._getOwner(), times(1)).basicInvalidatePart2(any(), any(), anyBoolean(),
        anyBoolean());
    verify(arm._getOwner(), never()).invokeInvalidateCallbacks(any(), any(), anyBoolean());
  }

  @Test
  public void invalidateForceNewEntryOfAlreadyInvalidEntryReturnsFalseWithForce() {
    AbstractRegionMap.FORCE_INVALIDATE_EVENT = true;
    try {
      TestableAbstractRegionMap arm = new TestableAbstractRegionMap();
      EntryEventImpl event = createEventForInvalidate(arm._getOwner());

      // invalidate on region that is not initialized should create
      // entry in map as invalid.
      assertTrue(arm.invalidate(event, true, true, false));
      verify(arm._getOwner(), times(1)).basicInvalidatePart2(any(), any(), anyBoolean(),
          anyBoolean());
      verify(arm._getOwner(), never()).invokeInvalidateCallbacks(any(), any(), anyBoolean());

      when(arm._getOwner().isInitialized()).thenReturn(true);
      assertFalse(arm.invalidate(event, true, true, false));
      verify(arm._getOwner(), times(1)).basicInvalidatePart2(any(), any(), anyBoolean(),
          anyBoolean());
      verify(arm._getOwner(), times(1)).invokeInvalidateCallbacks(any(), any(), anyBoolean());
    } finally {
      AbstractRegionMap.FORCE_INVALIDATE_EVENT = false;
    }
  }

  @Test
  public void destroyWithEmptyRegionThrowsException() {
    TestableAbstractRegionMap arm = new TestableAbstractRegionMap();
    EntryEventImpl event = createEventForDestroy(arm._getOwner());
    assertThatThrownBy(() -> arm.destroy(event, false, false, false, false, null, false))
        .isInstanceOf(EntryNotFoundException.class);
  }

  @Test
  public void destroyWithEmptyRegionInTokenModeAddsAToken() {
    final TestableAbstractRegionMap arm = new TestableAbstractRegionMap();
    final EntryEventImpl event = createEventForDestroy(arm._getOwner());
    final Object expectedOldValue = null;
    final boolean inTokenMode = true;
    final boolean duringRI = false;
    assertThat(arm.destroy(event, inTokenMode, duringRI, false, false, expectedOldValue, false))
        .isTrue();
    assertThat(arm._getMap().containsKey(event.getKey())).isTrue();
    RegionEntry re = (RegionEntry) arm._getMap().get(event.getKey());
    assertThat(re.getValueAsToken()).isEqualTo(Token.DESTROYED);
    boolean invokeCallbacks = true;
    verify(arm._getOwner(), times(1)).basicDestroyPart2(any(), eq(event), eq(inTokenMode),
        eq(false), eq(duringRI), eq(invokeCallbacks));
    verify(arm._getOwner(), times(1)).basicDestroyPart3(any(), eq(event), eq(inTokenMode),
        eq(duringRI), eq(invokeCallbacks), eq(expectedOldValue));
  }

  @Test
  public void destroyOfExistingEntryInTokenModeAddsAToken() {
    final TestableAbstractRegionMap arm = new TestableAbstractRegionMap();
    addEntry(arm);
    final EntryEventImpl event = createEventForDestroy(arm._getOwner());
    final Object expectedOldValue = null;
    final boolean inTokenMode = true;
    final boolean duringRI = false;
    assertThat(arm.destroy(event, inTokenMode, duringRI, false, false, expectedOldValue, false))
        .isTrue();
    assertThat(arm._getMap().containsKey(event.getKey())).isTrue();
    RegionEntry re = (RegionEntry) arm._getMap().get(event.getKey());
    assertThat(re.getValueAsToken()).isEqualTo(Token.DESTROYED);
    boolean invokeCallbacks = true;
    verify(arm._getOwner(), times(1)).basicDestroyPart2(any(), eq(event), eq(inTokenMode),
        eq(false), eq(duringRI), eq(invokeCallbacks));
    verify(arm._getOwner(), times(1)).basicDestroyPart3(any(), eq(event), eq(inTokenMode),
        eq(duringRI), eq(invokeCallbacks), eq(expectedOldValue));
  }

  @Test
  public void destroyOfExistingEntryRemovesEntryFromMapAndDoesNotifications() {
    final TestableAbstractRegionMap arm = new TestableAbstractRegionMap();
    addEntry(arm);
    final EntryEventImpl event = createEventForDestroy(arm._getOwner());
    final Object expectedOldValue = null;
    final boolean inTokenMode = false;
    final boolean duringRI = false;
    assertThat(arm.destroy(event, inTokenMode, duringRI, false, false, expectedOldValue, false))
        .isTrue();
    assertThat(arm._getMap().containsKey(event.getKey())).isFalse();
    boolean invokeCallbacks = true;
    verify(arm._getOwner(), times(1)).basicDestroyPart2(any(), eq(event), eq(inTokenMode),
        eq(false), eq(duringRI), eq(invokeCallbacks));
    verify(arm._getOwner(), times(1)).basicDestroyPart3(any(), eq(event), eq(inTokenMode),
        eq(duringRI), eq(invokeCallbacks), eq(expectedOldValue));
  }

  @Test
  public void destroyOfExistingEntryWithConcurrencyChecksAndNoVersionTagDestroysWithoutTombstone() {
    final TestableAbstractRegionMap arm = new TestableAbstractRegionMap(true);
    addEntry(arm);
    final EntryEventImpl event = createEventForDestroy(arm._getOwner());
    final Object expectedOldValue = null;
    final boolean inTokenMode = false;
    final boolean duringRI = false;
    assertThat(arm.destroy(event, inTokenMode, duringRI, false, false, expectedOldValue, false))
        .isTrue();
    // This might be a bug. It seems like we should have created a tombstone but we have no
    // version tag so that might be the cause of this bug.
    assertThat(arm._getMap().containsKey(event.getKey())).isFalse();
    boolean invokeCallbacks = true;
    verify(arm._getOwner(), times(1)).basicDestroyPart2(any(), eq(event), eq(inTokenMode),
        eq(false), eq(duringRI), eq(invokeCallbacks));
    verify(arm._getOwner(), times(1)).basicDestroyPart3(any(), eq(event), eq(inTokenMode),
        eq(duringRI), eq(invokeCallbacks), eq(expectedOldValue));
  }

  @Test
  public void destroyOfExistingEntryWithConcurrencyChecksAddsTombstone() {
    final TestableAbstractRegionMap arm = new TestableAbstractRegionMap(true);
    RegionVersionVector versionVector = mock(RegionVersionVector.class);
    when(arm._getOwner().getVersionVector()).thenReturn(versionVector);
    CachePerfStats cachePerfStats = mock(CachePerfStats.class);
    when(arm._getOwner().getCachePerfStats()).thenReturn(cachePerfStats);
    addEntry(arm);
    final EntryEventImpl event = createEventForDestroy(arm._getOwner());
    VersionTag versionTag = mock(VersionTag.class);
    when(versionTag.hasValidVersion()).thenReturn(true);
    event.setVersionTag(versionTag);
    final Object expectedOldValue = null;
    final boolean inTokenMode = false;
    final boolean duringRI = false;
    assertThat(arm.destroy(event, inTokenMode, duringRI, false, false, expectedOldValue, false))
        .isTrue();
    assertThat(arm._getMap().containsKey(event.getKey())).isTrue();
    RegionEntry re = (RegionEntry) arm._getMap().get(event.getKey());
    assertThat(re.getValueAsToken()).isEqualTo(Token.TOMBSTONE);
    boolean invokeCallbacks = true;
    verify(arm._getOwner(), times(1)).basicDestroyPart2(any(), eq(event), eq(inTokenMode),
        eq(false), eq(duringRI), eq(invokeCallbacks));
    verify(arm._getOwner(), times(1)).basicDestroyPart3(any(), eq(event), eq(inTokenMode),
        eq(duringRI), eq(invokeCallbacks), eq(expectedOldValue));
  }

  @Test
  public void evictDestroyOfExistingEntryWithConcurrencyChecksAddsTombstone() {
    final TestableVMLRURegionMap arm = new TestableVMLRURegionMap(true);
    RegionVersionVector<?> versionVector = mock(RegionVersionVector.class);
    when(arm._getOwner().getVersionVector()).thenReturn(versionVector);
    CachePerfStats cachePerfStats = mock(CachePerfStats.class);
    when(arm._getOwner().getCachePerfStats()).thenReturn(cachePerfStats);
    addEntry(arm);
    final EntryEventImpl event = createEventForDestroy(arm._getOwner());
    VersionTag<?> versionTag = mock(VersionTag.class);
    when(versionTag.hasValidVersion()).thenReturn(true);
    event.setVersionTag(versionTag);
    final Object expectedOldValue = null;
    final boolean inTokenMode = false;
    final boolean duringRI = false;
    final boolean evict = true;
    assertThat(arm.destroy(event, inTokenMode, duringRI, false, evict, expectedOldValue, false))
        .isTrue();
    assertThat(arm._getMap().containsKey(event.getKey())).isTrue();
    RegionEntry re = (RegionEntry) arm._getMap().get(event.getKey());
    assertThat(re.getValueAsToken()).isEqualTo(Token.TOMBSTONE);
    boolean invokeCallbacks = true;
    verify(arm._getOwner(), times(1)).basicDestroyPart2(any(), eq(event), eq(inTokenMode),
        eq(false), eq(duringRI), eq(invokeCallbacks));
    verify(arm._getOwner(), times(1)).basicDestroyPart3(any(), eq(event), eq(inTokenMode),
        eq(duringRI), eq(invokeCallbacks), eq(expectedOldValue));
  }


  private void addEntry(AbstractRegionMap arm) {
    RegionEntry entry = arm.getEntryFactory().createEntry(arm._getOwner(), KEY, "value");
    arm._getMap().put(KEY, entry);
  }

  @Test
  public void destroyWithEmptyRegionWithConcurrencyChecksThrowsException() {
    TestableAbstractRegionMap arm = new TestableAbstractRegionMap(true);
    EntryEventImpl event = createEventForDestroy(arm._getOwner());
    assertThatThrownBy(() -> arm.destroy(event, false, false, false, false, null, false))
        .isInstanceOf(EntryNotFoundException.class);
  }

  @Test
  public void evictDestroyWithEmptyRegionWithConcurrencyChecksDoesNothing() {
    final TestableVMLRURegionMap arm = new TestableVMLRURegionMap(true);
    EntryEventImpl event = createEventForDestroy(arm._getOwner());
    assertThat(arm.destroy(event, false, false, false, true, null, false)).isFalse();
    verify(arm._getOwner(), never()).basicDestroyPart2(any(), any(), anyBoolean(), anyBoolean(),
        anyBoolean(), anyBoolean());
    verify(arm._getOwner(), never()).basicDestroyPart3(any(), any(), anyBoolean(), anyBoolean(),
        anyBoolean(), any());
    // This seems to be a bug. We should not leave an entry in the map
    // added by the destroy call if destroy returns false.
    assertThat(arm._getMap().containsKey(event.getKey())).isTrue();
    RegionEntry re = (RegionEntry) arm._getMap().get(event.getKey());
    assertThat(re.getValueAsToken()).isEqualTo(Token.REMOVED_PHASE1);
  }

  @Test
  public void evictDestroyWithEmptyRegionDoesNothing() {
    final TestableVMLRURegionMap arm = new TestableVMLRURegionMap(false);
    EntryEventImpl event = createEventForDestroy(arm._getOwner());
    assertThat(arm.destroy(event, false, false, false, true, null, false)).isFalse();
    verify(arm._getOwner(), never()).basicDestroyPart2(any(), any(), anyBoolean(), anyBoolean(),
        anyBoolean(), anyBoolean());
    verify(arm._getOwner(), never()).basicDestroyPart3(any(), any(), anyBoolean(), anyBoolean(),
        anyBoolean(), any());
    assertThat(arm._getMap().containsKey(event.getKey())).isFalse();
  }

  @Test
  public void destroyWithEmptyRegionWithConcurrencyChecksAddsATombstone() {
    final TestableAbstractRegionMap arm = new TestableAbstractRegionMap(true);
    RegionVersionVector versionVector = mock(RegionVersionVector.class);
    when(arm._getOwner().getVersionVector()).thenReturn(versionVector);
    CachePerfStats cachePerfStats = mock(CachePerfStats.class);
    when(arm._getOwner().getCachePerfStats()).thenReturn(cachePerfStats);
    final EntryEventImpl event = createEventForDestroy(arm._getOwner());
    VersionTag versionTag = mock(VersionTag.class);
    when(versionTag.hasValidVersion()).thenReturn(true);
    event.setVersionTag(versionTag);
    event.setOriginRemote(true);
    final Object expectedOldValue = null;
    final boolean inTokenMode = false;
    final boolean duringRI = false;
    assertThat(arm.destroy(event, inTokenMode, duringRI, false, false, expectedOldValue, false))
        .isTrue();
    assertThat(arm._getMap().containsKey(event.getKey())).isTrue();
    RegionEntry re = (RegionEntry) arm._getMap().get(event.getKey());
    assertThat(re.getValueAsToken()).isEqualTo(Token.TOMBSTONE);
    boolean invokeCallbacks = true;
    verify(arm._getOwner(), times(1)).basicDestroyPart2(any(), eq(event), eq(inTokenMode),
        eq(false), eq(duringRI), eq(invokeCallbacks));
    verify(arm._getOwner(), times(1)).basicDestroyPart3(any(), eq(event), eq(inTokenMode),
        eq(duringRI), eq(invokeCallbacks), eq(expectedOldValue));
  }

  @Test
  public void destroyWithEmptyRegionWithConcurrencyChecksAndNullVersionTagAddsATombstone() {
    final TestableAbstractRegionMap arm = new TestableAbstractRegionMap(true);
    final EntryEventImpl event = createEventForDestroy(arm._getOwner());
    event.setOriginRemote(true);
    final Object expectedOldValue = null;
    final boolean inTokenMode = false;
    final boolean duringRI = false;
    assertThat(arm.destroy(event, inTokenMode, duringRI, false, false, expectedOldValue, false))
        .isTrue();
    assertThat(arm._getMap().containsKey(event.getKey())).isTrue();
    boolean invokeCallbacks = true;
    verify(arm._getOwner(), times(1)).basicDestroyPart2(any(), eq(event), eq(inTokenMode),
        eq(false), eq(duringRI), eq(invokeCallbacks));
    verify(arm._getOwner(), times(1)).basicDestroyPart3(any(), eq(event), eq(inTokenMode),
        eq(duringRI), eq(invokeCallbacks), eq(expectedOldValue));
    // instead of a TOMBSTONE we leave an entry whose value is REMOVE_PHASE1
    // this looks like a bug. It is caused by some code in: AbstractRegionEntry.destroy()
    // that calls removePhase1 when the versionTag is null.
    // It seems like this code path needs to tell the higher levels
    // to call removeEntry
    RegionEntry re = (RegionEntry) arm._getMap().get(event.getKey());
    assertThat(re.getValueAsToken()).isEqualTo(Token.REMOVED_PHASE1);
  }

  private static class TestableAbstractRegionMap extends AbstractRegionMap {

    protected TestableAbstractRegionMap() {
      this(false);
    }

    protected TestableAbstractRegionMap(boolean withConcurrencyChecks) {
      super(null);
      LocalRegion owner = mock(LocalRegion.class);
      when(owner.getConcurrencyChecksEnabled()).thenReturn(withConcurrencyChecks);
      when(owner.getDataPolicy()).thenReturn(DataPolicy.REPLICATE);
      doThrow(EntryNotFoundException.class).when(owner).checkEntryNotFound(any());
      initialize(owner, new Attributes(), null, false);
    }
  }

  private static class TestableVMLRURegionMap extends VMLRURegionMap {
    private static EvictionAttributes evictionAttributes =
        EvictionAttributes.createLRUEntryAttributes();

    protected TestableVMLRURegionMap() {
      this(false);
    }

    private static LocalRegion createOwner(boolean withConcurrencyChecks) {
      LocalRegion owner = mock(LocalRegion.class);
      when(owner.getEvictionAttributes()).thenReturn(evictionAttributes);
      when(owner.getConcurrencyChecksEnabled()).thenReturn(withConcurrencyChecks);
      when(owner.getDataPolicy()).thenReturn(DataPolicy.REPLICATE);
      doThrow(EntryNotFoundException.class).when(owner).checkEntryNotFound(any());
      return owner;
    }

    private static EvictionController createEvictionController() {
      EvictionController result = mock(EvictionController.class);
      when(result.getEvictionAlgorithm()).thenReturn(evictionAttributes.getAlgorithm());
      EvictionCounters evictionCounters = mock(EvictionCounters.class);
      when(result.getCounters()).thenReturn(evictionCounters);
      return result;
    }

    protected TestableVMLRURegionMap(boolean withConcurrencyChecks) {
      super(createOwner(withConcurrencyChecks), new Attributes(), null, createEvictionController());
    }
  }

  private static class TxTestableAbstractRegionMap extends AbstractRegionMap {

    protected TxTestableAbstractRegionMap() {
      super(null);
      LocalRegion owner = mock(LocalRegion.class);
      when(owner.getCache()).thenReturn(mock(InternalCache.class));
      when(owner.isAllEvents()).thenReturn(true);
      when(owner.isInitialized()).thenReturn(true);
      initialize(owner, new Attributes(), null, false);
    }
  }

  @Test
  public void txApplyInvalidateDoesNotInvalidateRemovedToken() throws RegionClearedException {
    TxTestableAbstractRegionMap arm = new TxTestableAbstractRegionMap();

    Object newValue = "value";
    arm.txApplyPut(Operation.CREATE, KEY, newValue, false,
        new TXId(mock(InternalDistributedMember.class), 1), mock(TXRmtEvent.class),
        mock(EventID.class), null, null, null, null, null, null, 1);
    RegionEntry re = arm.getEntry(KEY);
    assertNotNull(re);

    Token[] removedTokens =
        {Token.REMOVED_PHASE2, Token.REMOVED_PHASE1, Token.DESTROYED, Token.TOMBSTONE};

    for (Token token : removedTokens) {
      verifyTxApplyInvalidate(arm, KEY, re, token);
    }

  }

  private void verifyTxApplyInvalidate(TxTestableAbstractRegionMap arm, Object key, RegionEntry re,
      Token token) throws RegionClearedException {
    re.setValue(arm._getOwner(), token);
    arm.txApplyInvalidate(key, Token.INVALID, false,
        new TXId(mock(InternalDistributedMember.class), 1), mock(TXRmtEvent.class), false,
        mock(EventID.class), null, null, null, null, null, null, 1);
    assertEquals(re.getValueAsToken(), token);
  }

}
