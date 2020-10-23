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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisDataException;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.RedisPortSupplier;

public abstract class AbstractIncrIntegrationTest implements RedisPortSupplier {

  private Jedis jedis;
  private Jedis jedis2;
  private static final int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

  @Before
  public void setUp() {
    jedis = new Jedis("localhost", getPort(), REDIS_CLIENT_TIMEOUT);
    jedis2 = new Jedis("localhost", getPort(), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void tearDown() {
    jedis.flushAll();
    jedis.close();
    jedis2.close();
  }

  @Test
  public void givenKeyNotProvided_returnsWrongNumberOfArgumentsError() {
    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.INCR))
        .hasMessageContaining("ERR wrong number of arguments for 'incr' command");
  }

  @Test
  public void givenMoreThanTwoArgumentsProvided_returnsWrongNumberOfArgumentsError() {
    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.INCR, "key", "extraArg"))
        .hasMessageContaining("ERR wrong number of arguments for 'incr' command");
  }

  @Test
  public void testIncr() {
    String oneHundredKey = randString();
    String negativeOneHundredKey = randString();
    String unsetKey = randString();
    final int oneHundredValue = 100;
    final int negativeOneHundredValue = -100;
    jedis.set(oneHundredKey, Integer.toString(oneHundredValue));
    jedis.set(negativeOneHundredKey, Integer.toString(negativeOneHundredValue));

    jedis.incr(oneHundredKey);
    jedis.incr(negativeOneHundredKey);
    jedis.incr(unsetKey);

    assertThat(jedis.get(oneHundredKey)).isEqualTo(Integer.toString(oneHundredValue + 1));
    assertThat(jedis.get(negativeOneHundredKey))
        .isEqualTo(Integer.toString(negativeOneHundredValue + 1));
    assertThat(jedis.get(unsetKey)).isEqualTo(Integer.toString(1));
  }

  @Test
  public void testIncr_whenOverflow_shouldReturnError() {
    String key = "key";
    String max64BitIntegerValue = "9223372036854775807";
    jedis.set(key, max64BitIntegerValue);

    try {
      jedis.incr(key);
    } catch (JedisDataException e) {
      assertThat(e.getMessage()).contains(RedisConstants.ERROR_OVERFLOW);
    }
    assertThat(jedis.get(key)).isEqualTo(max64BitIntegerValue);
  }

  @Test
  public void testIncr_whenWrongType_shouldReturnError() {
    String key = "key";
    String nonIntegerValue = "I am not a number! I am a free man!";
    assertThat(jedis.set(key, nonIntegerValue)).isEqualTo("OK");

    try {
      jedis.incr(key);
    } catch (JedisDataException e) {
      assertThat(e.getMessage()).contains("out of range");
    }
    assertThat(jedis.get(key)).isEqualTo(nonIntegerValue);
  }

  @Test
  public void testIncr_shouldBeAtomic() {
    jedis.set("contestedKey", "0");

    int ITERATION_COUNT = 4000;
    new ConcurrentLoopingThreads(ITERATION_COUNT,
        (i) -> jedis.incr("contestedKey"),
        (i) -> jedis2.incr("contestedKey"))
            .run();

    assertThat(jedis.get("contestedKey")).isEqualTo(Integer.toString(2 * ITERATION_COUNT));
  }

  private String randString() {
    return Long.toHexString(Double.doubleToLongBits(Math.random()));
  }
}
