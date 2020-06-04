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

import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.redis.internal.RedisConstants.ArityDef;
import org.apache.geode.redis.internal.RedisResponse;
import org.apache.geode.redis.internal.data.ByteArrayWrapper;

public class BitCountExecutor extends StringExecutor {

  private final String ERROR_NOT_INT = "The indexes provided must be numeric values";

  @Override
  public RedisResponse executeCommandWithResponse(Command command,
      ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() != 2 && commandElems.size() != 4) {
      return RedisResponse.error(ArityDef.BITCOUNT);
    }

    ByteArrayWrapper key = command.getKey();
    RedisStringCommands stringCommands = getRedisStringCommands(context);
    ByteArrayWrapper wrapper = stringCommands.get(key);
    if (wrapper == null) {
      return RedisResponse.integer(0);
    }
    byte[] value = wrapper.toBytes();

    long startL = 0;
    long endL = value.length - 1;

    if (commandElems.size() == 4) {
      try {
        startL = Coder.bytesToLong(commandElems.get(2));
        endL = Coder.bytesToLong(commandElems.get(3));
      } catch (NumberFormatException e) {
        return RedisResponse.error(ERROR_NOT_INT);
      }
    }
    if (startL > Integer.MAX_VALUE || endL > Integer.MAX_VALUE) {
      return RedisResponse.error(RedisConstants.ERROR_OUT_OF_RANGE);
    }

    int start = (int) startL;
    int end = (int) endL;
    if (start < 0) {
      start += value.length;
    }
    if (end < 0) {
      end += value.length;
    }

    if (start < 0) {
      start = 0;
    }
    if (end < 0) {
      end = 0;
    }

    if (end > value.length - 1) {
      end = value.length - 1;
    }

    if (end < start || start >= value.length) {
      return RedisResponse.integer(0);
    }

    long setBits = 0;
    for (int j = start; j <= end; j++) {
      setBits += Integer.bitCount(0xFF & value[j]); // 0xFF keeps same bit sequence as the byte as
    }
    // opposed to keeping the same value

    return RedisResponse.integer(setBits);
  }

}
