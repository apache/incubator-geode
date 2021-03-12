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

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

public class RegionProviderJUnitTest {

  @Test
  public void testBucket_whenPowerOfTwo() {
    assertThatNoException().isThrownBy(() -> RegionProvider.validateBuckets(128));
  }

  @Test
  public void testException_whenNotPowerOfTwo() {
    assertThatThrownBy(() -> RegionProvider.validateBuckets(127))
        .hasMessageContaining("redis region buckets must be a power of 2");
  }

  @Test
  public void testException_whenGreaterThanSlots() {
    assertThatThrownBy(() -> RegionProvider.validateBuckets(32768))
        .hasMessageContaining("redis region buckets must <= 16384");
  }

}