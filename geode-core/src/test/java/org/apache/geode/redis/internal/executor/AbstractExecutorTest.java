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
package org.apache.geode.redis.internal.executor;

import static org.junit.Assert.*;

import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisDataType;
import org.apache.geode.redis.internal.RegionProvider;
import org.apache.geode.redis.internal.executor.string.SetExecutor;
import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

/**
 * Test for AbstractExecutor
 * 
 *
 */
@Category(UnitTest.class)
public class AbstractExecutorTest {

  /**
   * Test the remove entry mehtod
   */
  @Test
  public void testRemoveEntry() {
    // Create any instance of the AbstractExecutor
    AbstractExecutor abstractExecutor = new SetExecutor();

    // setup mocks
    ExecutionHandlerContext context = Mockito.mock(ExecutionHandlerContext.class);
    RegionProvider rp = Mockito.mock(RegionProvider.class);
    Mockito.when(context.getRegionProvider()).thenReturn(rp);
    Mockito.when(rp.removeKey(Mockito.any())).thenReturn(true);

    // Assert false to protected or null types
    assertFalse(abstractExecutor.removeEntry(Coder.stringToByteArrayWrapper("junit"),
        RedisDataType.REDIS_PROTECTED, context));

    assertFalse(
        abstractExecutor.removeEntry(Coder.stringToByteArrayWrapper("junit"), null, context));



  }

}
