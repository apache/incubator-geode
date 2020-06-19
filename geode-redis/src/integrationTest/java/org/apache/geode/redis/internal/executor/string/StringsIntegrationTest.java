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

import static java.lang.Integer.parseInt;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static redis.clients.jedis.Protocol.Command.SET;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.assertj.core.api.SoftAssertions;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import redis.clients.jedis.BitOP;
import redis.clients.jedis.BitPosParams;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.params.SetParams;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.GeodeRedisServerRule;
import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.junit.categories.RedisTest;

@Category({RedisTest.class})
public class StringsIntegrationTest {

  static Jedis jedis;
  static Jedis jedis2;
  static Random rand;
  private static int ITERATION_COUNT = 4000;

  @ClassRule
  public static GeodeRedisServerRule server = new GeodeRedisServerRule();

  @BeforeClass
  public static void setUp() {
    rand = new Random();
    jedis = new Jedis("localhost", server.getPort(), 10000000);
    jedis2 = new Jedis("localhost", server.getPort(), 10000000);
  }

  @After
  public void flushAll() {
    jedis.flushAll();
  }

  @AfterClass
  public static void tearDown() {
    jedis.close();
    jedis2.close();
  }

  @Test
  public void testSET_shouldSetStringValueToKey_givenEmptyKey() {

    String key = "key";
    String value = "value";

    String result = jedis.get(key);
    assertThat(result).isNull();

    jedis.set(key, value);
    result = jedis.get(key);
    assertThat(result).isEqualTo(value);
  }

  @Test
  public void testSET_shouldSetStringValueToKey_givenKeyIsOfDataTypeSet() {
    String key = "key";
    String stringValue = "value";

    jedis.sadd(key, "member1", "member2");

    jedis.set(key, stringValue);

    assertThat(jedis.get(key)).isEqualTo(stringValue);
  }

  @Test
  public void testSET_shouldSetStringValueToKey_givenKeyIsOfDataTypeHash() {
    String key = "key";
    String stringValue = "value";

    jedis.hset(key, "field", "something else");

    String result = jedis.set(key, stringValue);
    assertThat(result).isEqualTo("OK");

    assertThat(jedis.get(key)).isEqualTo(stringValue);
  }

  @Test
  public void testSET_withNXAndExArguments() {
    String key = "key";
    String stringValue = "value";

    SetParams setParams = new SetParams();
    setParams.nx();
    setParams.ex(20);

    jedis.set(key, stringValue, setParams);
    assertThat(jedis.ttl(key)).isGreaterThan(15);
    assertThat(jedis.get(key)).isEqualTo(stringValue);
  }

  @Test
  public void testSET_withXXAndExArguments() {
    String key = "key";
    String stringValue = "value";

    jedis.set(key, "differentValue");

    SetParams setParams = new SetParams();
    setParams.xx();
    setParams.ex(20);

    jedis.set(key, stringValue, setParams);
    assertThat(jedis.ttl(key)).isGreaterThan(15);
    assertThat(jedis.get(key)).isEqualTo(stringValue);
  }

  @Test
  public void testSET_withNXAndPxArguments() {
    String key = "key";
    String stringValue = "value";

    SetParams setParams = new SetParams();
    setParams.nx();
    setParams.px(2000);

    jedis.set(key, stringValue, setParams);
    assertThat(jedis.pttl(key)).isGreaterThan(1500);
    assertThat(jedis.get(key)).isEqualTo(stringValue);
  }

  @Test
  public void testSET_withXXAndPxArguments() {
    String key = "key";
    String stringValue = "value";

    jedis.set(key, "differentValue");

    SetParams setParams = new SetParams();
    setParams.xx();
    setParams.px(2000);

    jedis.set(key, stringValue, setParams);
    assertThat(jedis.pttl(key)).isGreaterThan(1500);
    assertThat(jedis.get(key)).isEqualTo(stringValue);
  }

  @Test
  public void testSET_withNXArgument_shouldReturnNil_ifKeyContainsOtherDataType() {
    String key = "key";
    String stringValue = "value";

    jedis.sadd(key, "member1", "member2");
    SetParams setParams = new SetParams();
    setParams.nx();

    String result = jedis.set(key, stringValue, setParams);
    assertThat(result).isNull();
  }

  @Test
  public void testSET_shouldSetXX_ifKeyContainsOtherDataType() {
    String key = "key";
    String stringValue = "value";

    jedis.sadd(key, "member1", "member2");
    SetParams setParams = new SetParams();
    setParams.xx();

    jedis.set(key, stringValue, setParams);
    String result = jedis.get(key);

    assertThat(result).isEqualTo(stringValue);
  }

  @Test
  public void setNX_shouldNotConflictWithRegularSet() {
    List<String> keys = new ArrayList<>();
    List<String> values = new ArrayList<>();
    for (int i = 0; i < ITERATION_COUNT; i++) {
      keys.add("key-" + i);
      values.add("value-" + i);
    }

    AtomicInteger counter = new AtomicInteger(0);
    SetParams setParams = new SetParams();
    setParams.nx();

    new ConcurrentLoopingThreads(ITERATION_COUNT,
        (i) -> {
          String ok = jedis.set(keys.get(i), values.get(i));
          if ("OK".equals(ok)) {
            counter.addAndGet(1);
          }
        },
        (i) -> jedis2.set(keys.get(i), values.get(i), setParams))
            .run();

    assertThat(counter.get()).isEqualTo(ITERATION_COUNT);
  }

  @Test
  public void testSET_withEXArgument_shouldSetExpireTime() {
    String key = "key";
    String value = "value";
    int secondsUntilExpiration = 20;

    SetParams setParams = new SetParams();
    setParams.ex(secondsUntilExpiration);

    jedis.set(key, value, setParams);

    Long result = jedis.ttl(key);

    assertThat(result).isGreaterThan(15l);
  }

  @Test
  public void testSET_withNegativeEXTime_shouldReturnError() {
    String key = "key";
    String value = "value";
    int millisecondsUntilExpiration = -1;

    SetParams setParams = new SetParams();
    setParams.ex(millisecondsUntilExpiration);

    assertThatThrownBy(() -> jedis.set(key, value, setParams))
        .isInstanceOf(JedisDataException.class)
        .hasMessageContaining(RedisConstants.ERROR_INVALID_EXPIRE_TIME);
  }

  @Test
  public void testSET_withPXArgument_shouldSetExpireTime() {
    String key = "key";
    String value = "value";
    int millisecondsUntilExpiration = 20000;

    SetParams setParams = new SetParams();
    setParams.px(millisecondsUntilExpiration);

    jedis.set(key, value, setParams);

    Long result = jedis.ttl(key);

    assertThat(result).isGreaterThan(15l);
  }

  @Test
  public void testSET_withNegativePXTime_shouldReturnError() {
    String key = "key";
    String value = "value";
    int millisecondsUntilExpiration = -1;

    SetParams setParams = new SetParams();
    setParams.px(millisecondsUntilExpiration);

    assertThatThrownBy(() -> jedis.set(key, value, setParams))
        .isInstanceOf(JedisDataException.class)
        .hasMessageContaining(RedisConstants.ERROR_INVALID_EXPIRE_TIME);
  }

  @Test
  public void testSET_shouldClearPreviousTTL() {
    String key = "key";
    String value = "value";
    int secondsUntilExpiration = 20;

    SetParams setParams = new SetParams();
    setParams.ex(secondsUntilExpiration);

    jedis.set(key, value, setParams);

    jedis.set(key, "other value");

    Long result = jedis.ttl(key);

    assertThat(result).isEqualTo(-1L);
  }

  @Test
  public void testSET_withXXArgument_shouldClearPreviousTTL() {
    String key = "xx_key";
    String value = "did exist";
    int secondsUntilExpiration = 20;
    SetParams setParamsXX = new SetParams();
    setParamsXX.xx();
    SetParams setParamsEX = new SetParams();
    setParamsEX.ex(secondsUntilExpiration);
    String result_EX = jedis.set(key, value, setParamsEX);
    assertThat(result_EX).isEqualTo("OK");
    assertThat(jedis.ttl(key)).isGreaterThan(15L);

    String result_XX = jedis.set(key, value, setParamsXX);

    assertThat(result_XX).isEqualTo("OK");
    Long result = jedis.ttl(key);
    assertThat(result).isEqualTo(-1L);
  }

  @Test
  public void testSET_should_not_clearPreviousTTL_onFailure() {
    String key_NX = "nx_key";
    String value_NX = "set only if key did not exist";
    int secondsUntilExpiration = 20;

    SetParams setParamsEX = new SetParams();
    setParamsEX.ex(secondsUntilExpiration);

    SetParams setParamsNX = new SetParams();
    setParamsNX.nx();

    jedis.set(key_NX, value_NX, setParamsEX);
    String result_NX = jedis.set(key_NX, value_NX, setParamsNX);
    assertThat(result_NX).isNull();

    Long result = jedis.ttl(key_NX);
    assertThat(result).isGreaterThan(15L);
  }

  @Test
  @Ignore("KEEPTTL is part of redis 6")
  public void testSET_with_KEEPTTL_shouldRetainPreviousTTL_OnSuccess() {
    String key = "key";
    String value = "value";
    int secondsToExpire = 30;

    SetParams setParamsEx = new SetParams();
    setParamsEx.ex(secondsToExpire);

    jedis.set(key, value, setParamsEx);

    SetParams setParamsKeepTTL = new SetParams();
    // setParamsKeepTTL.keepTtl();
    // Jedis Doesn't support KEEPTTL yet.

    jedis.set(key, "newValue", setParamsKeepTTL);

    Long result = jedis.ttl(key);
    assertThat(result).isGreaterThan(15L);
  }

  @Test
  public void set_with_KEEPTTL_fails() {
    assertThatThrownBy(() -> jedis.sendCommand(SET, "key", "value", "KEEPTTL"))
        .hasMessageContaining("syntax error");
  }

  @Test
  public void testSET_withNXArgument_shouldOnlySetKeyIfKeyDoesNotExist() {
    String key1 = "key_1";
    String key2 = "key_2";
    String value1 = "value_1";
    String value2 = "value_2";

    jedis.set(key1, value1);

    SetParams setParams = new SetParams();
    setParams.nx();

    jedis.set(key1, value2, setParams);
    String result1 = jedis.get(key1);

    assertThat(result1).isEqualTo(value1);

    jedis.set(key2, value2, setParams);
    String result2 = jedis.get(key2);

    assertThat(result2).isEqualTo(value2);
  }

  @Test
  public void testSET_withXXArgument_shouldOnlySetKeyIfKeyExists() {
    String key1 = "key_1";
    String key2 = "key_2";
    String value1 = "value_1";
    String value2 = "value_2";

    jedis.set(key1, value1);

    SetParams setParams = new SetParams();
    setParams.xx();

    jedis.set(key1, value2, setParams);
    String result1 = jedis.get(key1);

    assertThat(result1).isEqualTo(value2);

    jedis.set(key2, value2, setParams);
    String result2 = jedis.get(key2);

    assertThat(result2).isNull();
  }

  @Test
  public void testSET_XXAndNXArguments_shouldReturnOK_ifSuccessful() {
    String key_NX = "nx_key";
    String key_XX = "xx_key";
    String value_NX = "did not exist";
    String value_XX = "did exist";

    SetParams setParamsXX = new SetParams();
    setParamsXX.xx();

    SetParams setParamsNX = new SetParams();
    setParamsNX.nx();

    String result_NX = jedis.set(key_NX, value_NX, setParamsNX);
    assertThat(result_NX).isEqualTo("OK");

    jedis.set(key_XX, value_XX);
    String result_XX = jedis.set(key_NX, value_NX, setParamsXX);
    assertThat(result_XX).isEqualTo("OK");
  }

  @Test
  public void testSET_XXAndNXArguments_shouldReturnNull_ifNotSuccessful() {
    String key_NX = "nx_key";
    String key_XX = "xx_key";
    String value_NX = "set only if key did not exist";
    String value_XX = "set only if key did exist";

    SetParams setParamsXX = new SetParams();
    setParamsXX.xx();

    SetParams setParamsNX = new SetParams();
    setParamsNX.nx();

    jedis.set(key_NX, value_NX);
    String result_NX = jedis.set(key_NX, value_NX, setParamsNX);
    assertThat(result_NX).isNull();

    String result_XX = jedis.set(key_XX, value_XX, setParamsXX);
    assertThat(result_XX).isNull();
  }

  @Test
  public void testGET_shouldReturnValueOfKey_givenValueIsAString() {
    String key = "key";
    String value = "value";

    String result = jedis.get(key);
    assertThat(result).isNull();

    jedis.set(key, value);
    result = jedis.get(key);
    assertThat(result).isEqualTo(value);
  }



  @Test
  public void testSET_withInvalidOptions() {
    SoftAssertions soft = new SoftAssertions();

    soft.assertThatThrownBy(() -> jedis.sendCommand(SET))
        .as("invalid options #1")
        .isInstanceOf(JedisDataException.class);

    soft.assertThatThrownBy(() -> jedis.sendCommand(SET, "foo", "EX", "0"))
        .as("invalid options #2")
        .isInstanceOf(JedisDataException.class)
        .hasMessageContaining("syntax error");

    soft.assertThatThrownBy(() -> jedis.sendCommand(SET, "foo", "bar", "EX", "a"))
        .as("invalid options #3")
        .isInstanceOf(JedisDataException.class)
        .hasMessageContaining("value is not an integer");

    soft.assertThatThrownBy(() -> jedis.sendCommand(SET, "foo", "bar", "PX", "1", "EX", "0"))
        .as("invalid options #4")
        .isInstanceOf(JedisDataException.class)
        .hasMessageContaining("syntax error");

    soft.assertThatThrownBy(() -> jedis.sendCommand(SET, "foo", "bar", "PX", "1", "XX", "0"))
        .as("invalid options #5")
        .isInstanceOf(JedisDataException.class)
        .hasMessageContaining("syntax error");

    soft.assertThatThrownBy(() -> jedis.sendCommand(SET, "foo", "bar", "PX", "XX", "0"))
        .as("invalid options #6")
        .isInstanceOf(JedisDataException.class)
        .hasMessageContaining("syntax error");

    soft.assertThatThrownBy(() -> jedis.sendCommand(SET, "foo", "bar", "1", "PX", "1"))
        .as("invalid options #7")
        .isInstanceOf(JedisDataException.class)
        .hasMessageContaining("syntax error");

    soft.assertThatThrownBy(() -> jedis.sendCommand(SET, "foo", "bar", "NX", "XX"))
        .as("invalid options #8")
        .isInstanceOf(JedisDataException.class)
        .hasMessageContaining("syntax error");

    soft.assertAll();
  }



  @Test
  public void testGET_shouldReturnNil_givenKeyIsEmpty() {
    String key = "this key does not exist";

    String result = jedis.get(key);
    assertThat(result).isNull();
  }

  @Test
  public void testGET_shouldThrowJedisDataExceptionError_givenValueIsNotAString() {
    String key = "key";
    String field = "field";
    String member = "member";

    jedis.sadd(key, field, member);

    assertThatThrownBy(() -> jedis.get(key))
        .hasMessageContaining("Operation against a key holding the wrong kind of value");
  }

  @Test
  public void testAppend_shouldAppendValueWithInputStringAndReturnResultingLength() {
    String key = "key";
    String value = randString();
    int originalValueLength = value.length();

    boolean result = jedis.exists(key);
    assertThat(result).isFalse();

    Long output = jedis.append(key, value);
    assertThat(output).isEqualTo(originalValueLength);

    String randomString = randString();

    output = jedis.append(key, randomString);
    assertThat(output).isEqualTo(originalValueLength + randomString.length());

    String finalValue = jedis.get(key);
    assertThat(finalValue).isEqualTo(value.concat(randomString));
  }

  @Test
  public void testAppend_concurrent() {
    int listSize = 1000;
    String key = "key";

    List<String> values1 = makeStringList(listSize, "values1-");
    List<String> values2 = makeStringList(listSize, "values2-");

    new ConcurrentLoopingThreads(listSize,
        (i) -> jedis.append(key, values1.get(i)),
        (i) -> jedis2.append(key, values2.get(i))).run();

    for (int i = 0; i < listSize; i++) {
      assertThat(jedis.get(key)).contains(values1.get(i));
      assertThat(jedis.get(key)).contains(values2.get(i));
    }
  }


  @Test
  public void testGetRange_whenWholeRangeSpecified_returnsEntireValue() {
    String key = "key";
    String valueWith19Characters = "abc123babyyouknowme";

    jedis.set(key, valueWith19Characters);

    String everything = jedis.getrange(key, 0, -1);
    assertThat(everything).isEqualTo(valueWith19Characters);

    String alsoEverything = jedis.getrange(key, 0, 18);
    assertThat(alsoEverything).isEqualTo(valueWith19Characters);

  }

  @Test
  public void testGetRange_whenMoreThanWholeRangeSpecified_returnsEntireValue() {
    String key = "key";
    String valueWith19Characters = "abc123babyyouknowme";

    jedis.set(key, valueWith19Characters);

    String fromStartToWayPastEnd = jedis.getrange(key, 0, 5000);
    assertThat(fromStartToWayPastEnd).isEqualTo(valueWith19Characters);

    String wayBeforeStartAndJustToEnd = jedis.getrange(key, -50000, -1);
    assertThat(wayBeforeStartAndJustToEnd).isEqualTo(valueWith19Characters);

    String wayBeforeStartAndWayAfterEnd = jedis.getrange(key, -50000, 5000);
    assertThat(wayBeforeStartAndWayAfterEnd).isEqualTo(valueWith19Characters);
  }

  @Test
  public void testGetRange_whenValidSubrangeSpecified_returnsAppropriateSubstring() {
    String key = "key";
    String valueWith19Characters = "abc123babyyouknowme";

    jedis.set(key, valueWith19Characters);

    String fromStartToBeforeEnd = jedis.getrange(key, 0, 16);
    assertThat(fromStartToBeforeEnd).isEqualTo("abc123babyyouknow");

    String fromStartByNegativeOffsetToBeforeEnd = jedis.getrange(key, -19, 16);
    assertThat(fromStartByNegativeOffsetToBeforeEnd).isEqualTo("abc123babyyouknow");

    String fromStartToBeforeEndByNegativeOffset = jedis.getrange(key, 0, -3);
    assertThat(fromStartToBeforeEndByNegativeOffset).isEqualTo("abc123babyyouknow");

    String fromAfterStartToBeforeEnd = jedis.getrange(key, 2, 16);
    assertThat(fromAfterStartToBeforeEnd).isEqualTo("c123babyyouknow");

    String fromAfterStartByNegativeOffsetToBeforeEndByNegativeOffset = jedis.getrange(key, -16, -2);
    assertThat(fromAfterStartByNegativeOffsetToBeforeEndByNegativeOffset)
        .isEqualTo("123babyyouknowm");

    String fromAfterStartToEnd = jedis.getrange(key, 2, 18);
    assertThat(fromAfterStartToEnd).isEqualTo("c123babyyouknowme");

    String fromAfterStartToEndByNegativeOffset = jedis.getrange(key, 2, -1);
    assertThat(fromAfterStartToEndByNegativeOffset).isEqualTo("c123babyyouknowme");
  }

  @Test
  public void testGetRange_rangeIsInvalid_returnsEmptyString() {
    String key = "key";
    String valueWith19Characters = "abc123babyyouknowme";

    jedis.set(key, valueWith19Characters);

    String range1 = jedis.getrange(key, -2, -16);
    assertThat(range1).isEqualTo("");

    String range2 = jedis.getrange(key, 2, 0);
    assertThat(range2).isEqualTo("");
  }

  @Test
  public void testGetRange_nonexistentKey_returnsEmptyString() {
    String key = "nonexistent";

    String range = jedis.getrange(key, 0, -1);
    assertThat(range).isEqualTo("");
  }

  @Test
  public void testGetRange_rangePastEndOfValue_returnsEmptyString() {
    String key = "key";
    String value = "value";

    jedis.set(key, value);

    String range = jedis.getrange(key, 7, 14);
    assertThat(range).isEqualTo("");
  }

  @Test
  public void setRange_replacesStart() {
    jedis.set("key", "0123456789");
    assertThat(jedis.setrange("key", 0, "abcd")).isEqualTo(10);
    assertThat(jedis.get("key")).isEqualTo("abcd456789");
  }

  @Test
  public void setRange_replacesMiddle() {
    jedis.set("key", "0123456789");
    assertThat(jedis.setrange("key", 3, "abc")).isEqualTo(10);
    assertThat(jedis.get("key")).isEqualTo("012abc6789");
  }

  @Test
  public void setRange_replacesEnd() {
    jedis.set("key", "0123456789");
    assertThat(jedis.setrange("key", 7, "abc")).isEqualTo(10);
    assertThat(jedis.get("key")).isEqualTo("0123456abc");
  }

  @Test
  public void setRange_extendsEnd() {
    jedis.set("key", "0123456789");
    assertThat(jedis.setrange("key", 10, "abc")).isEqualTo(13);
    assertThat(jedis.get("key")).isEqualTo("0123456789abc");
  }

  @Test
  public void setRange_extendsAndPadsWithZero() {
    jedis.set("key", "0123456789");
    assertThat(jedis.setrange("key", 11, "abc")).isEqualTo(14);
    assertThat((int) (jedis.get("key").charAt(10))).isEqualTo(0);
  }

  @Test
  public void setRange_createsKey() {
    assertThat(jedis.setrange("key", 0, "abcd")).isEqualTo(4);
    assertThat(jedis.get("key")).isEqualTo("abcd");
  }

  @Test
  public void setRange_givenSetFails() {
    jedis.sadd("key", "m1");
    assertThatThrownBy(() -> jedis.setrange("key", 0, "abc")).hasMessageContaining("WRONGTYPE");
  }

  @Test
  public void bitcount_givenSetFails() {
    jedis.sadd("key", "m1");
    assertThatThrownBy(() -> jedis.bitcount("key")).hasMessageContaining("WRONGTYPE");
  }

  @Test
  public void bitcount_givenNonExistentKeyReturnsZero() {
    assertThat(jedis.bitcount("does not exist")).isEqualTo(0);
    assertThat(jedis.exists("does not exist")).isFalse();
  }

  @Test
  public void bitcount_givenEmptyStringReturnsZero() {
    jedis.set("key", "");
    assertThat(jedis.bitcount("key")).isEqualTo(0);
  }

  @Test
  public void bitcount_givenOneBitReturnsOne() {
    byte[] key = {1, 2, 3};
    byte[] bytes = {1, 0, 0, 0, 0};
    jedis.set(key, bytes);
    assertThat(jedis.bitcount(key)).isEqualTo(1);
  }

  @Test
  public void bitcount_givenTwoBitsReturnsTwo() {
    byte[] key = {1, 2, 3};
    byte[] bytes = {1, 0, 0, 0, 1};
    jedis.set(key, bytes);
    assertThat(jedis.bitcount(key)).isEqualTo(2);
  }

  @Test
  public void bitcount_givenEmptyRangeReturnsZero() {
    byte[] key = {1, 2, 3};
    byte[] bytes = {1, 0, 0, 0, 1};
    jedis.set(key, bytes);
    assertThat(jedis.bitcount(key, 1, 3)).isEqualTo(0);
  }

  @Test
  public void bitcount_correctForAllByteValues() {
    byte[] key = {1, 2, 3};
    byte[] value = {0};
    for (int b = Byte.MIN_VALUE; b <= Byte.MAX_VALUE; b++) {
      value[0] = (byte) b;
      jedis.set(key, value);
      assertThat(jedis.bitcount(key)).as("b=" + b).isEqualTo(Integer.bitCount(0xFF & b));
    }
  }

  @Test
  public void bitpos_givenSetFails() {
    jedis.sadd("key", "m1");
    assertThatThrownBy(() -> jedis.bitpos("key", false)).hasMessageContaining("WRONGTYPE");
    assertThatThrownBy(() -> jedis.bitpos("key", true)).hasMessageContaining("WRONGTYPE");
  }

  @Test
  public void bitpos_givenNonExistentKeyReturnsExpectedValue() {
    assertThat(jedis.bitpos("does not exist", false)).isEqualTo(0);
    assertThat(jedis.bitpos("does not exist", true)).isEqualTo(-1);
    assertThat(jedis.bitpos("does not exist", false, new BitPosParams(4, 7))).isEqualTo(0);
    assertThat(jedis.bitpos("does not exist", true, new BitPosParams(4, 7))).isEqualTo(-1);
    assertThat(jedis.exists("does not exist")).isFalse();
  }

  @Test
  public void bitcount_givenBitInFirstByte() {
    byte[] key = {1, 2, 3};
    byte[] bytes = {1, 1, 1, 1, 1};
    jedis.set(key, bytes);
    assertThat(jedis.bitpos(key, true)).isEqualTo(7);
  }

  @Test
  public void bitcount_givenOneInSecondByte() {
    byte[] key = {1, 2, 3};
    byte[] bytes = {0, 1, 1, 1, 1};
    jedis.set(key, bytes);
    assertThat(jedis.bitpos(key, true)).isEqualTo(7 + 8);
  }

  @Test
  public void bitcountFalse_givenBitInFirstByte() {
    byte[] key = {1, 2, 3};
    byte[] bytes = {-2, 1, 1, 1, 1};
    jedis.set(key, bytes);
    assertThat(jedis.bitpos(key, false)).isEqualTo(7);
  }

  @Test
  public void bitcountFalse_givenOneInSecondByte() {
    byte[] key = {1, 2, 3};
    byte[] bytes = {-1, -2, 1, 1, 1};
    jedis.set(key, bytes);
    assertThat(jedis.bitpos(key, false)).isEqualTo(7 + 8);
  }

  @Test
  public void bitcountWithStart_givenOneInLastByte() {
    byte[] key = {1, 2, 3};
    byte[] bytes = {1, 1, 1, 1};
    jedis.set(key, bytes);
    assertThat(jedis.bitpos(key, true, new BitPosParams(-1))).isEqualTo(7 + 3 * 8);
  }

  @Test
  public void bitcountWithStartAndEnd_givenNoBits() {
    byte[] key = {1, 2, 3};
    byte[] bytes = {1, 0, 0, 1};
    jedis.set(key, bytes);
    assertThat(jedis.bitpos(key, true, new BitPosParams(1, 2))).isEqualTo(-1);
  }


  @Test
  public void getbit_givenSetFails() {
    jedis.sadd("key", "m1");
    assertThatThrownBy(() -> jedis.getbit("key", 1)).hasMessageContaining("WRONGTYPE");
  }

  @Test
  public void getbit_givenNonExistentKeyReturnsFalse() {
    assertThat(jedis.getbit("does not exist", 1)).isFalse();
    assertThat(jedis.exists("does not exist")).isFalse();
  }

  @Test
  public void getbit_givenNoBitsReturnsFalse() {
    byte[] key = {1, 2, 3};
    byte[] bytes = {0};
    jedis.set(key, bytes);
    assertThat(jedis.getbit(key, 1)).isFalse();
  }

  @Test
  public void getbit_givenOneBitReturnsTrue() {
    byte[] key = {1, 2, 3};
    byte[] bytes = {0, 1};
    jedis.set(key, bytes);
    assertThat(jedis.getbit(key, 8 + 7)).isTrue();
  }

  @Test
  public void getbit_pastEndReturnsFalse() {
    byte[] key = {1, 2, 3};
    byte[] bytes = {0, 1};
    jedis.set(key, bytes);
    assertThat(jedis.getbit(key, 8 + 8 + 7)).isFalse();
  }

  @Test
  public void setbit_givenSetFails() {
    jedis.sadd("key", "m1");
    assertThatThrownBy(() -> jedis.setbit("key", 1, true)).hasMessageContaining("WRONGTYPE");
  }

  @Test
  public void setbit_givenNonExistentKeyCreatesString() {
    assertThat(jedis.setbit("newKey", 1, true)).isFalse();
    assertThat(jedis.exists("newKey")).isTrue();
    assertThat(jedis.type("newKey")).isEqualTo("string");
    assertThat(jedis.getbit("newKey", 1)).isTrue();
  }

  @Test
  public void setbit_canSetOneBit() {
    byte[] key = {1, 2, 3};
    byte[] bytes = {0};
    jedis.set(key, bytes);
    assertThat(jedis.setbit(key, 1, true)).isFalse();
    byte[] newbytes = jedis.get(key);
    assertThat(newbytes[0]).isEqualTo((byte) 0x40);
  }

  @Test
  public void setbit_canSetOneBitAlreadySet() {
    byte[] key = {1, 2, 3};
    byte[] bytes = {1};
    jedis.set(key, bytes);
    assertThat(jedis.setbit(key, 7, true)).isTrue();
    byte[] newbytes = jedis.get(key);
    assertThat(newbytes[0]).isEqualTo((byte) 1);
  }

  @Test
  public void setbit_canSetOneBitPastEnd() {
    byte[] key = {1, 2, 3};
    byte[] bytes = {0};
    jedis.set(key, bytes);
    assertThat(jedis.setbit(key, 1 + 8, true)).isFalse();
    byte[] newbytes = jedis.get(key);
    assertThat(newbytes[0]).isEqualTo((byte) 0);
    assertThat(newbytes[1]).isEqualTo((byte) 0x40);
  }

  @Test
  public void bitop_givenSetFails() {
    jedis.sadd("foo", "m1");
    assertThatThrownBy(() -> jedis.bitop(BitOP.AND, "key", "foo"))
        .hasMessageContaining("WRONGTYPE");
    assertThatThrownBy(() -> jedis.bitop(BitOP.OR, "key", "foo")).hasMessageContaining("WRONGTYPE");
    assertThatThrownBy(() -> jedis.bitop(BitOP.XOR, "key", "foo"))
        .hasMessageContaining("WRONGTYPE");
    assertThatThrownBy(() -> jedis.bitop(BitOP.NOT, "key", "foo"))
        .hasMessageContaining("WRONGTYPE");
  }

  @Test
  public void bitopNOT_givenNothingLeavesKeyUnset() {
    assertThat(jedis.bitop(BitOP.NOT, "key", "foo")).isEqualTo(0);
    assertThat(jedis.exists("key")).isFalse();
  }

  @Test
  public void bitopNOT_givenNothingDeletesKey() {
    jedis.set("key", "value");
    assertThat(jedis.bitop(BitOP.NOT, "key", "foo")).isEqualTo(0);
    assertThat(jedis.exists("key")).isFalse();
  }

  @Test
  public void bitopNOT_givenNothingDeletesSet() {
    jedis.sadd("key", "value");
    assertThat(jedis.bitop(BitOP.NOT, "key", "foo")).isEqualTo(0);
    assertThat(jedis.exists("key")).isFalse();
  }

  @Test
  public void bitopNOT_givenEmptyStringDeletesKey() {
    jedis.set("key", "value");
    jedis.set("foo", "");
    assertThat(jedis.bitop(BitOP.NOT, "key", "foo")).isEqualTo(0);
    assertThat(jedis.exists("key")).isFalse();
  }

  @Test
  public void bitopNOT_givenEmptyStringDeletesSet() {
    jedis.sadd("key", "value");
    jedis.set("foo", "");
    assertThat(jedis.bitop(BitOP.NOT, "key", "foo")).isEqualTo(0);
    assertThat(jedis.exists("key")).isFalse();
  }

  @Test
  public void bitopNOT_negatesSelf() {
    byte[] key = {1, 2, 3};
    byte[] bytes = {1};
    jedis.set(key, bytes);
    assertThat(jedis.bitop(BitOP.NOT, key, key)).isEqualTo(1);
    assertThat(jedis.strlen(key)).isEqualTo(1);
    byte[] newbytes = jedis.get(key);
    assertThat(newbytes[0]).isEqualTo((byte) 0xFE);
  }

  @Test
  public void bitopNOT_createsNonExistingKey() {
    byte[] key = {1};
    byte[] other = {2};
    byte[] bytes = {1};
    jedis.set(other, bytes);
    assertThat(jedis.bitop(BitOP.NOT, key, other)).isEqualTo(1);
    assertThat(jedis.strlen(key)).isEqualTo(1);
    byte[] newbytes = jedis.get(key);
    assertThat(newbytes[0]).isEqualTo((byte) 0xFE);
  }

  @Test
  public void bitopAND_givenSelfAndOther() {
    byte[] key = {1};
    byte[] other = {2};
    byte[] bytes = {1};
    byte[] otherBytes = {-1};
    jedis.set(key, bytes);
    jedis.set(other, otherBytes);
    assertThat(jedis.bitop(BitOP.AND, key, key, other)).isEqualTo(1);
    assertThat(jedis.strlen(key)).isEqualTo(1);
    byte[] newbytes = jedis.get(key);
    assertThat(newbytes[0]).isEqualTo((byte) 1);
  }

  @Test
  public void bitopAND_givenSelfAndLongerOther() {
    byte[] key = {1};
    byte[] other = {2};
    byte[] bytes = {1};
    byte[] otherBytes = {-1, 3};
    jedis.set(key, bytes);
    jedis.set(other, otherBytes);
    assertThat(jedis.bitop(BitOP.AND, key, key, other)).isEqualTo(2);
    assertThat(jedis.strlen(key)).isEqualTo(2);
    byte[] newbytes = jedis.get(key);
    assertThat(newbytes[0]).isEqualTo((byte) 1);
    assertThat(newbytes[1]).isEqualTo((byte) 0);
  }

  @Test
  public void bitopOR_givenSelfAndOther() {
    byte[] key = {1};
    byte[] other = {2};
    byte[] bytes = {1};
    byte[] otherBytes = {8};
    jedis.set(key, bytes);
    jedis.set(other, otherBytes);
    assertThat(jedis.bitop(BitOP.OR, key, key, other)).isEqualTo(1);
    assertThat(jedis.strlen(key)).isEqualTo(1);
    byte[] newbytes = jedis.get(key);
    assertThat(newbytes[0]).isEqualTo((byte) 9);
  }

  @Test
  public void bitopOR_givenSelfAndLongerOther() {
    byte[] key = {1};
    byte[] other = {2};
    byte[] bytes = {1};
    byte[] otherBytes = {-1, 3};
    jedis.set(key, bytes);
    jedis.set(other, otherBytes);
    assertThat(jedis.bitop(BitOP.OR, key, key, other)).isEqualTo(2);
    assertThat(jedis.strlen(key)).isEqualTo(2);
    byte[] newbytes = jedis.get(key);
    assertThat(newbytes[0]).isEqualTo((byte) -1);
    assertThat(newbytes[1]).isEqualTo((byte) 3);
  }

  @Test
  public void bitopXOR_givenSelfAndOther() {
    byte[] key = {1};
    byte[] other = {2};
    byte[] bytes = {9};
    byte[] otherBytes = {8};
    jedis.set(key, bytes);
    jedis.set(other, otherBytes);
    assertThat(jedis.bitop(BitOP.XOR, key, key, other)).isEqualTo(1);
    assertThat(jedis.strlen(key)).isEqualTo(1);
    byte[] newbytes = jedis.get(key);
    assertThat(newbytes[0]).isEqualTo((byte) 1);
  }

  @Test
  public void bitopXOR_givenSelfAndLongerOther() {
    byte[] key = {1};
    byte[] other = {2};
    byte[] bytes = {1};
    byte[] otherBytes = {-1, 3};
    jedis.set(key, bytes);
    jedis.set(other, otherBytes);
    assertThat(jedis.bitop(BitOP.XOR, key, key, other)).isEqualTo(2);
    assertThat(jedis.strlen(key)).isEqualTo(2);
    byte[] newbytes = jedis.get(key);
    assertThat(newbytes[0]).isEqualTo((byte) 0xFE);
    assertThat(newbytes[1]).isEqualTo((byte) 3);
  }


  @Test
  public void testGetSet_updatesKeyWithNewValue_returnsOldValue() {
    String key = randString();
    String contents = randString();
    jedis.set(key, contents);

    String newContents = randString();
    String oldContents = jedis.getSet(key, newContents);
    assertThat(oldContents).isEqualTo(contents);

    contents = newContents;
    newContents = jedis.get(key);
    assertThat(newContents).isEqualTo(contents);
  }

  @Test
  public void testGetSet_setsNonexistentKeyToNewValue_returnsNull() {
    String key = randString();
    String newContents = randString();

    String oldContents = jedis.getSet(key, newContents);
    assertThat(oldContents).isNull();

    String contents = jedis.get(key);
    assertThat(newContents).isEqualTo(contents);
  }

  @Test
  public void testGetSet_shouldWorkWith_INCR_Command() {
    String key = "key";
    Long resultLong;
    String resultString;

    jedis.set(key, "0");

    resultLong = jedis.incr(key);
    assertThat(resultLong).isEqualTo(1);

    resultString = jedis.getSet(key, "0");
    assertThat(parseInt(resultString)).isEqualTo(1);

    resultString = jedis.get(key);
    assertThat(parseInt(resultString)).isEqualTo(0);

    resultLong = jedis.incr(key);
    assertThat(resultLong).isEqualTo(1);
  }

  @Test
  public void testGetSet_whenWrongType_shouldReturnError() {
    String key = "key";
    jedis.hset(key, "field", "some hash value");

    assertThatThrownBy(() -> jedis.getSet(key, "this value doesn't matter"))
        .isInstanceOf(JedisDataException.class)
        .hasMessageContaining(RedisConstants.ERROR_WRONG_TYPE);
  }

  @Test
  public void testGetSet_shouldBeAtomic()
      throws ExecutionException, InterruptedException, TimeoutException {
    jedis.set("contestedKey", "0");
    assertThat(jedis.get("contestedKey")).isEqualTo("0");
    CountDownLatch latch = new CountDownLatch(1);
    ExecutorService pool = Executors.newFixedThreadPool(2);
    Callable<Integer> callable1 = () -> doABunchOfIncrs(jedis, latch);
    Callable<Integer> callable2 = () -> doABunchOfGetSets(jedis2, latch);
    Future<Integer> future1 = pool.submit(callable1);
    Future<Integer> future2 = pool.submit(callable2);

    latch.countDown();

    GeodeAwaitility.await().untilAsserted(() -> assertThat(future2.get()).isEqualTo(future1.get()));
    assertThat(future1.get() + future2.get()).isEqualTo(2 * ITERATION_COUNT);
  }

  private Integer doABunchOfIncrs(Jedis jedis, CountDownLatch latch) throws InterruptedException {
    latch.await();
    for (int i = 0; i < ITERATION_COUNT; i++) {
      jedis.incr("contestedKey");
    }
    return ITERATION_COUNT;
  }

  private Integer doABunchOfGetSets(Jedis jedis, CountDownLatch latch) throws InterruptedException {
    int sum = 0;
    latch.await();

    while (sum < ITERATION_COUNT) {
      sum += Integer.parseInt(jedis.getSet("contestedKey", "0"));
    }
    return sum;
  }

  @Test
  public void testDel_deletingOneKey_removesKeyAndReturnsOne() {
    String key1 = "firstKey";
    jedis.set(key1, randString());

    Long deletedCount = jedis.del(key1);

    assertThat(deletedCount).isEqualTo(1L);
    assertThat(jedis.get(key1)).isNull();
  }

  @Test
  public void testDel_deletingNonexistentKey_returnsZero() {
    assertThat(jedis.del("ceci nest pas un clavier")).isEqualTo(0L);
  }

  @Test
  public void testDel_deletingMultipleKeys_returnsCountOfOnlyDeletedKeys() {
    String key1 = "firstKey";
    String key2 = "secondKey";
    String key3 = "thirdKey";

    jedis.set(key1, randString());
    jedis.set(key2, randString());

    assertThat(jedis.del(key1, key2, key3)).isEqualTo(2L);
    assertThat(jedis.get(key1)).isNull();
    assertThat(jedis.get(key2)).isNull();
  }

  @Test
  public void testMSetAndMGet_forHappyPath_setsKeysAndReturnsCorrectValues() {
    int keyCount = 5;
    String[] keyvals = new String[(keyCount * 2)];
    String[] keys = new String[keyCount];
    String[] vals = new String[keyCount];
    for (int i = 0; i < keyCount; i++) {
      String key = randString();
      String val = randString();
      keyvals[2 * i] = key;
      keyvals[2 * i + 1] = val;
      keys[i] = key;
      vals[i] = val;
    }

    String resultString = jedis.mset(keyvals);
    assertThat(resultString).isEqualTo("OK");

    List<String> ret = jedis.mget(keys);
    Object[] retArray = ret.toArray();

    assertThat(Arrays.equals(vals, retArray)).isTrue();
  }

  @Test
  public void testMGet_requestNonexistentKey_respondsWithNil() {
    String key1 = "existingKey";
    String key2 = "notReallyAKey";
    String value1 = "theRealValue";
    String[] keys = new String[2];
    String[] expectedVals = new String[2];
    keys[0] = key1;
    keys[1] = key2;
    expectedVals[0] = value1;
    expectedVals[1] = null;

    jedis.set(key1, value1);

    List<String> ret = jedis.mget(keys);
    Object[] retArray = ret.toArray();

    assertThat(Arrays.equals(expectedVals, retArray)).isTrue();
  }

  @Test
  @Ignore("GEODE-8192")
  public void testMSet_concurrentInstances_mustBeAtomic()
      throws InterruptedException, ExecutionException {
    String keyBaseName = "MSETBASE";
    String val1BaseName = "FIRSTVALBASE";
    String val2BaseName = "SECONDVALBASE";
    String[] keysAndVals1 = new String[(ITERATION_COUNT * 2)];
    String[] keysAndVals2 = new String[(ITERATION_COUNT * 2)];
    String[] keys = new String[ITERATION_COUNT];
    String[] vals1 = new String[ITERATION_COUNT];
    String[] vals2 = new String[ITERATION_COUNT];
    String[] expectedVals;

    SetUpArraysForConcurrentMSet(keyBaseName,
        val1BaseName, val2BaseName,
        keysAndVals1, keysAndVals2,
        keys,
        vals1, vals2);

    RunTwoMSetsInParallelThreadsAndVerifyReturnValue(keysAndVals1, keysAndVals2);

    List<String> actualVals = jedis.mget(keys);
    expectedVals = DetermineWhichMSetWonTheRace(vals1, vals2, actualVals);

    assertThat(actualVals.toArray(new String[] {})).contains(expectedVals);
  }

  private void SetUpArraysForConcurrentMSet(String keyBaseName, String val1BaseName,
      String val2BaseName, String[] keysAndVals1,
      String[] keysAndVals2, String[] keys, String[] vals1,
      String[] vals2) {
    for (int i = 0; i < ITERATION_COUNT; i++) {
      String key = keyBaseName + i;
      String value1 = val1BaseName + i;
      String value2 = val2BaseName + i;
      keysAndVals1[2 * i] = key;
      keysAndVals1[2 * i + 1] = value1;
      keysAndVals2[2 * i] = key;
      keysAndVals2[2 * i + 1] = value2;
      keys[i] = key;
      vals1[i] = value1;
      vals2[i] = value2;
    }
  }

  private void RunTwoMSetsInParallelThreadsAndVerifyReturnValue(String[] keysAndVals1,
      String[] keysAndVals2)
      throws InterruptedException, ExecutionException {
    CountDownLatch latch = new CountDownLatch(1);
    ExecutorService pool = Executors.newFixedThreadPool(2);
    Callable<String> callable1 = () -> jedis.mset(keysAndVals1);
    Callable<String> callable2 = () -> jedis2.mset(keysAndVals2);
    Future<String> future1 = pool.submit(callable1);
    Future<String> future2 = pool.submit(callable2);

    latch.countDown();

    assertThat(future1.get()).isEqualTo("OK");
    assertThat(future2.get()).isEqualTo("OK");
  }

  private String[] DetermineWhichMSetWonTheRace(String[] vals1, String[] vals2,
      List<String> actualVals) {
    String[] expectedVals;
    if (actualVals.get(0).equals("FIRSTVALBASE0")) {
      expectedVals = vals1;
    } else {
      expectedVals = vals2;
    }
    return expectedVals;
  }

  @Test
  public void testConcurrentDel_differentClients() {
    String keyBaseName = "DELBASE";

    new ConcurrentLoopingThreads(
        ITERATION_COUNT,
        (i) -> jedis.set(keyBaseName + i, "value" + i))
            .run();

    AtomicLong deletedCount = new AtomicLong();
    new ConcurrentLoopingThreads(ITERATION_COUNT,
        (i) -> deletedCount.addAndGet(jedis.del(keyBaseName + i)),
        (i) -> deletedCount.addAndGet(jedis2.del(keyBaseName + i)))
            .run();


    assertThat(deletedCount.get()).isEqualTo(ITERATION_COUNT);

    for (int i = 0; i < ITERATION_COUNT; i++) {
      assertThat(jedis.get(keyBaseName + i)).isNull();
    }

  }

  @Test
  public void testMSetNX() {
    Set<String> keysAndVals = new HashSet<String>();
    for (int i = 0; i < 2 * 5; i++) {
      keysAndVals.add(randString());
    }
    String[] keysAndValsArray = keysAndVals.toArray(new String[0]);
    long response = jedis.msetnx(keysAndValsArray);

    assertThat(response).isEqualTo(1);

    long response2 = jedis.msetnx(keysAndValsArray[0], randString());

    assertThat(response2).isEqualTo(0);
    assertThat(keysAndValsArray[1]).isEqualTo(jedis.get(keysAndValsArray[0]));
  }

  @Test
  public void testDecr() {
    String oneHundredKey = randString();
    String negativeOneHundredKey = randString();
    String unsetKey = randString();
    final int oneHundredValue = 100;
    final int negativeOneHundredValue = -100;
    jedis.set(oneHundredKey, Integer.toString(oneHundredValue));
    jedis.set(negativeOneHundredKey, Integer.toString(negativeOneHundredValue));

    jedis.decr(oneHundredKey);
    jedis.decr(negativeOneHundredKey);
    jedis.decr(unsetKey);

    assertThat(jedis.get(oneHundredKey)).isEqualTo(Integer.toString(oneHundredValue - 1));
    assertThat(jedis.get(negativeOneHundredKey))
        .isEqualTo(Integer.toString(negativeOneHundredValue - 1));
    assertThat(jedis.get(unsetKey)).isEqualTo(Integer.toString(-1));
  }

  @Test
  public void testDecr_shouldBeAtomic() throws ExecutionException, InterruptedException {
    jedis.set("contestedKey", "0");

    new ConcurrentLoopingThreads(
        ITERATION_COUNT,
        (i) -> jedis.decr("contestedKey"),
        (i) -> jedis2.decr("contestedKey"))
            .run();

    assertThat(jedis.get("contestedKey")).isEqualTo(Integer.toString(-2 * ITERATION_COUNT));
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
      assertThat(e.getMessage()).contains(RedisConstants.ERROR_NOT_INTEGER);
    }
    assertThat(jedis.get(key)).isEqualTo(nonIntegerValue);
  }

  @Test
  public void testIncr_shouldBeAtomic() throws ExecutionException, InterruptedException {
    jedis.set("contestedKey", "0");

    new ConcurrentLoopingThreads(
        ITERATION_COUNT,
        (i) -> jedis.incr("contestedKey"),
        (i) -> jedis2.incr("contestedKey"))
            .run();

    assertThat(jedis.get("contestedKey")).isEqualTo(Integer.toString(2 * ITERATION_COUNT));
  }

  @Test
  public void testDecrBy() {
    String key1 = randString();
    String key2 = randString();
    String key3 = randString();
    int decr1 = rand.nextInt(100);
    int decr2 = rand.nextInt(100);
    Long decr3 = Long.MAX_VALUE / 2;
    int num1 = 100;
    int num2 = -100;
    jedis.set(key1, "" + num1);
    jedis.set(key2, "" + num2);
    jedis.set(key3, "" + Long.MIN_VALUE);

    jedis.decrBy(key1, decr1);
    jedis.decrBy(key2, decr2);

    assertThat(jedis.get(key1)).isEqualTo("" + (num1 - decr1 * 1));
    assertThat(jedis.get(key2)).isEqualTo("" + (num2 - decr2 * 1));

    Exception ex = null;
    try {
      jedis.decrBy(key3, decr3);
    } catch (Exception e) {
      ex = e;
    }
    assertThat(ex).isNotNull();

  }

  @Test
  public void testSetNX() {
    String key1 = randString();
    String key2;
    do {
      key2 = randString();
    } while (key2.equals(key1));

    long response1 = jedis.setnx(key1, key1);
    long response2 = jedis.setnx(key2, key2);
    long response3 = jedis.setnx(key1, key2);

    assertThat(response1).isEqualTo(1);
    assertThat(response2).isEqualTo(1);
    assertThat(response3).isEqualTo(0);
  }

  @Test
  public void testIncrBy() {
    String key1 = randString();
    String key2 = randString();
    String key3 = randString();
    int incr1 = rand.nextInt(100);
    int incr2 = rand.nextInt(100);
    Long incr3 = Long.MAX_VALUE / 2;
    int num1 = 100;
    int num2 = -100;
    jedis.set(key1, "" + num1);
    jedis.set(key2, "" + num2);
    jedis.set(key3, "" + Long.MAX_VALUE);

    jedis.incrBy(key1, incr1);
    jedis.incrBy(key2, incr2);
    assertThat(jedis.get(key1)).isEqualTo("" + (num1 + incr1 * 1));
    assertThat(jedis.get(key2)).isEqualTo("" + (num2 + incr2 * 1));

    Exception ex = null;
    try {
      jedis.incrBy(key3, incr3);
    } catch (Exception e) {
      ex = e;
    }
    assertThat(ex).isNotNull();
  }

  @Test
  public void testIncrByFloat() {
    String key1 = randString();
    String key2 = randString();
    double incr1 = rand.nextInt(100);
    double incr2 = rand.nextInt(100);
    double num1 = 100;
    double num2 = -100;
    jedis.set(key1, "" + num1);
    jedis.set(key2, "" + num2);

    jedis.incrByFloat(key1, incr1);
    jedis.incrByFloat(key2, incr2);
    assertThat(Double.valueOf(jedis.get(key1))).isEqualTo(num1 + incr1);
    assertThat(Double.valueOf(jedis.get(key2))).isEqualTo(num2 + incr2);
  }

  @Test
  public void testPAndSetex() {
    Random r = new Random();
    int setex = r.nextInt(5);
    if (setex == 0) {
      setex = 1;
    }
    String key = randString();
    jedis.setex(key, setex, randString());
    try {
      Thread.sleep((setex + 5) * 1000);
    } catch (InterruptedException e) {
      return;
    }
    String result = jedis.get(key);
    assertThat(result).isNull();

    int psetex = r.nextInt(5000);
    if (psetex == 0) {
      psetex = 1;
    }
    key = randString();
    jedis.psetex(key, psetex, randString());
    long start = System.currentTimeMillis();
    try {
      Thread.sleep(psetex + 5000);
    } catch (InterruptedException e) {
      return;
    }
    long stop = System.currentTimeMillis();
    result = jedis.get(key);
    assertThat(stop - start).isGreaterThanOrEqualTo(psetex);
    assertThat(result).isNull();
  }

  @Test
  public void testStrlen_requestNonexistentKey_returnsZero() {
    Long result = jedis.strlen("Nohbdy");
    assertThat(result).isEqualTo(0);
  }

  @Test
  public void testStrlen_requestKey_returnsLengthOfStringValue() {
    String value = "byGoogle";

    jedis.set("golang", value);

    Long result = jedis.strlen("golang");
    assertThat(result).isEqualTo(value.length());
  }

  @Test
  public void testStrlen_requestWrongType_shouldReturnError() {
    String key = "hashKey";
    jedis.hset(key, "field", "this value doesn't matter");

    assertThatThrownBy(() -> jedis.strlen(key))
        .isInstanceOf(JedisDataException.class)
        .hasMessageContaining(RedisConstants.ERROR_WRONG_TYPE);
  }

  private String randString() {
    return Long.toHexString(Double.doubleToLongBits(Math.random()));
  }

  private List<String> makeStringList(int setSize, String baseString) {
    List<String> strings = new ArrayList<>();
    for (int i = 0; i < setSize; i++) {
      strings.add(baseString + i);
    }
    return strings;
  }
}
