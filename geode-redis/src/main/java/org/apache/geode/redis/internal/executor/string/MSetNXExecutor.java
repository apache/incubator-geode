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
package org.apache.geode.redis.internal.executor.string;


import java.util.List;

import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisConstants.ArityDef;
import org.apache.geode.redis.internal.RedisResponse;
import org.apache.geode.redis.internal.data.ByteArrayWrapper;
import org.apache.geode.redis.internal.executor.key.RedisKeyCommands;

public class MSetNXExecutor extends StringExecutor {

  private final int SET = 1;

  private final int NOT_SET = 0;

  @Override
  public RedisResponse executeCommandWithResponse(Command command,
      ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 3 || commandElems.size() % 2 == 0) {
      return RedisResponse.error(ArityDef.MSETNX);
    }

    RedisStringCommands stringCommands = getRedisStringCommands(context);
    RedisKeyCommands keyCommands = getRedisKeyCommands(context);
    boolean successful = false;

    // TODO: make this atomic
    for (int i = 1; i < commandElems.size(); i += 2) {
      byte[] keyArray = commandElems.get(i);
      ByteArrayWrapper key = new ByteArrayWrapper(keyArray);
      if (keyCommands.exists(key)) {
        return RedisResponse.integer(NOT_SET);
      }
    }

    // none exist so now set them all
    for (int i = 1; i < commandElems.size(); i += 2) {
      byte[] keyArray = commandElems.get(i);
      ByteArrayWrapper key = new ByteArrayWrapper(keyArray);
      byte[] valueArray = commandElems.get(i + 1);
      ByteArrayWrapper value = new ByteArrayWrapper(valueArray);
      stringCommands.set(key, value, null);
    }

    return RedisResponse.integer(SET);
  }

}
