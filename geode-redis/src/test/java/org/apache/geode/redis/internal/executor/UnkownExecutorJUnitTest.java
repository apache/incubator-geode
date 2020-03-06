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
 *
 */
package org.apache.geode.redis.internal.executor;

import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;

/**
 * Test for the UnkownExecutor
 *
 *
 */
public class UnkownExecutorJUnitTest {
  /**
   * Test the execution method
   */
  @Test
  public void testExecuteCommand() {
    UnkownExecutor exe = new UnkownExecutor();

    Command command = Mockito.mock(Command.class);
    ExecutionHandlerContext context = Mockito.mock(ExecutionHandlerContext.class);


    UnpooledByteBufAllocator byteBuf = new UnpooledByteBufAllocator(false);
    Mockito.when(context.getByteBufAllocator()).thenReturn(byteBuf);

    exe.executeCommand(command, context);

    // verify the response was set
    Mockito.verify(command).setResponse(Mockito.any());

  }

}
