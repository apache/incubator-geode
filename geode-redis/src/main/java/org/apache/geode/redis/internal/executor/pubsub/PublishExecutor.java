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

package org.apache.geode.redis.internal.executor.pubsub;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.netty.buffer.ByteBuf;

import org.apache.geode.redis.internal.executor.AbstractExecutor;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Coder;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class PublishExecutor extends AbstractExecutor {

  private ExecutorService executorService = Executors.newSingleThreadExecutor();

  @Override
  public RedisResponse executeCommand(Command command,
      ExecutionHandlerContext context) {
    List<byte[]> args = command.getProcessedCommand();

    executorService.submit(new PublishingRunnable(context, args.get(1), args.get(2)));
    return null;
  }


  public static class PublishingRunnable implements Runnable {

    private final ExecutionHandlerContext context;
    private final byte[] channelName;
    private final byte[] message;

    public PublishingRunnable(ExecutionHandlerContext context, byte[] channelName, byte[] message) {
      this.context = context;
      this.channelName = channelName;
      this.message = message;
    }

    @Override
    public void run() {
      long publishCount =
          context.getPubSub()
              .publish(context.getRegionProvider().getDataRegion(), channelName, message);
      ByteBuf response = Coder.getIntegerResponse(context.getByteBufAllocator(), publishCount);
      context.writeToChannel(response);
    }
  }
}
