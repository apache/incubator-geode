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
package org.apache.geode.cache;

import static com.googlecode.catchexception.CatchException.catchException;
import static com.googlecode.catchexception.CatchException.caughtException;
import static org.apache.geode.cache.RegionShortcut.REPLICATE;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.DataSerializable;
import org.apache.geode.ToDataException;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxWriter;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedTestRule;
import org.apache.geode.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class BrokenSerializationConsistencyRegressionTest implements Serializable {

  private static final String REGION_NAME = "replicateRegion";
  private static final String REGION_NAME2 = "replicateRegion2";
  private static final String KEY = "key";

  private VM vm0;

  private transient FailsToSerialize valueFailsToSerialize;
  private transient FailsToPdxSerialize pdxValueFailsToSerialize;

  private transient String stringValue;
  private transient PdxValue pdxValue;
  private transient byte[] bytesValue;

  @ClassRule
  public static DistributedTestRule distributedTestRule = new DistributedTestRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Before
  public void setUp() {
    vm0 = getHost(0).getVM(0);

    // create replicate region in both
    vm0.invoke(() -> createReplicateRegions());
    createReplicateRegions();

    valueFailsToSerialize = new FailsToSerialize();
    pdxValueFailsToSerialize = new FailsToPdxSerialize();

    stringValue = "hello world";
    pdxValue = new PdxValue();
    bytesValue = new byte[] {0, 1, 2};
  }

  @Test
  public void failureToSerializeFailsToPropagate() {
    Region region = cacheRule.getCache().getRegion(REGION_NAME);
    catchException(region).put(KEY, valueFailsToSerialize);

    Exception caughtException = caughtException();
    assertThat(caughtException).isInstanceOf(ToDataException.class);
    assertThat(caughtException.getCause()).isInstanceOf(IOException.class)
        .hasMessage("FailsToSerialize");
    assertThat(region.get(KEY)).isNull();

    vm0.invoke(() -> {
      Region regionOnVm0 = cacheRule.getCache().getRegion(REGION_NAME);
      assertThat(regionOnVm0.get(KEY)).isNull();
    });
  }

  @Test
  public void failureToSerializeFailsToPropagateInTransaction() {
    Region region = cacheRule.getCache().getRegion(REGION_NAME);
    Region region2 = cacheRule.getCache().getRegion(REGION_NAME2);
    TXManagerImpl txManager = cacheRule.getCache().getTxManager();
    txManager.begin();
    region2.put(KEY, stringValue);
    region.put(KEY, valueFailsToSerialize);
    catchException(txManager).commit();

    Exception caughtException = caughtException();
    assertThat(caughtException).isInstanceOf(ToDataException.class);
    assertThat(caughtException.getCause()).isInstanceOf(IOException.class)
        .hasMessage("FailsToSerialize");
    assertThat(region.get(KEY)).isNull();
    assertThat(region2.get(KEY)).isNull();

    vm0.invoke(() -> {
      Region regionOnVm0 = cacheRule.getCache().getRegion(REGION_NAME);
      Region region2OnVm0 = cacheRule.getCache().getRegion(REGION_NAME2);
      assertThat(regionOnVm0.get(KEY)).isNull();
      assertThat(region2OnVm0.get(KEY)).isNull();
    });
  }

  @Test
  public void failureToPdxSerializeFails() {
    Region region = cacheRule.getCache().getRegion(REGION_NAME);
    catchException(region).put(KEY, pdxValueFailsToSerialize);

    Exception caughtException = caughtException();
    assertThat(caughtException).isInstanceOf(ToDataException.class);
    assertThat(caughtException.getCause()).isInstanceOf(UncheckedIOException.class);
    assertThat(caughtException.getCause().getCause()).isInstanceOf(IOException.class)
        .hasMessage("FailsToSerialize");
    assertThat(region.get(KEY)).isNull();

    vm0.invoke(() -> {
      Region regionOnVm0 = cacheRule.getCache().getRegion(REGION_NAME);
      assertThat(regionOnVm0.get(KEY)).isNull();
    });
  }

  private void createReplicateRegions() {
    RegionFactory regionFactory = cacheRule.getOrCreateCache().createRegionFactory(REPLICATE);
    regionFactory.create(REGION_NAME);
    regionFactory.create(REGION_NAME2);
  }

  private static class FailsToSerialize implements DataSerializable {

    @Override
    public void toData(DataOutput out) throws IOException {
      throw new IOException("FailsToSerialize");
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      // nothing
    }
  }

  private static class FailsToPdxSerialize implements PdxSerializable {

    @Override
    public void toData(PdxWriter writer) {
      throw new UncheckedIOException(new IOException("FailsToSerialize"));
    }

    @Override
    public void fromData(PdxReader reader) {
      // nothing
    }
  }

  public static class PdxValue implements PdxSerializable {

    @Override
    public void toData(PdxWriter writer) {
      // nothing
    }

    @Override
    public void fromData(PdxReader reader) {
      // nothing
    }
  }
}
