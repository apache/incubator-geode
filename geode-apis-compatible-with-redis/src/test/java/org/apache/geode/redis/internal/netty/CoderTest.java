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
package org.apache.geode.redis.internal.netty;

import static org.apache.geode.redis.internal.netty.Coder.bytesToDouble;
import static org.apache.geode.redis.internal.netty.Coder.bytesToString;
import static org.apache.geode.redis.internal.netty.Coder.doubleToString;
import static org.apache.geode.redis.internal.netty.Coder.equalsIgnoreCaseBytes;
import static org.apache.geode.redis.internal.netty.Coder.isInfinity;
import static org.apache.geode.redis.internal.netty.Coder.isNaN;
import static org.apache.geode.redis.internal.netty.Coder.isNegativeInfinity;
import static org.apache.geode.redis.internal.netty.Coder.isPositiveInfinity;
import static org.apache.geode.redis.internal.netty.Coder.stringToBytes;
import static org.apache.geode.redis.internal.netty.Coder.toUpperCaseBytes;
import static org.assertj.core.api.Assertions.assertThat;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(JUnitParamsRunner.class)
public class CoderTest {
  @Test
  @Parameters(method = "stringPairs")
  public void equalsIgnoreCaseBytes_matchesStringEqualsIgnoreCase(String string1, String string2) {
    byte[] string1Bytes = stringToBytes(string1);
    byte[] string2Bytes = stringToBytes(string2);
    assertThat(equalsIgnoreCaseBytes(string1Bytes, string2Bytes))
        .as("Comparing equality of " + string1 + " and " + string2)
        .isEqualTo(string1.equalsIgnoreCase(string2));
  }

  @Test
  @Parameters({"abc", "AbC", "ABC", "%abc", "123abc!@#", "+inf", "-INF"})
  public void toUpperCaseBytes_matchesStringToUpperCase(String string) {
    byte[] uppercase = toUpperCaseBytes(stringToBytes(string));
    assertThat(uppercase)
        .withFailMessage("Comparing toUpperCase for " + string
            + ".\nExpected: " + bytesToString(uppercase)
            + "\nActual: " + string.toUpperCase())
        .isEqualTo(stringToBytes(string.toUpperCase()));
  }

  @Test
  @Parameters(method = "infinityStrings")
  public void isInfinity_returnsCorrectly(String string, boolean isPositiveInfinity,
      boolean isNegativeInfinity, boolean isNaN) {
    byte[] bytes = stringToBytes(string);
    assertThat(isInfinity(bytes)).isEqualTo(isPositiveInfinity || isNegativeInfinity);
    assertThat(isPositiveInfinity(bytes)).isEqualTo(isPositiveInfinity);
    assertThat(isNegativeInfinity(bytes)).isEqualTo(isNegativeInfinity);
    assertThat(isNaN(bytes)).isEqualTo(isNaN);
  }

  @Test
  @Parameters(method = "infinityReturnStrings")
  public void doubleToString_processesLikeRedis(String inputString, String expectedString) {
    byte[] bytes = stringToBytes(inputString);
    assertThat(doubleToString(bytesToDouble(bytes))).isEqualTo(expectedString);
  }

  @SuppressWarnings("unused")
  private Object[] stringPairs() {
    // string1, string2
    return new Object[] {
        new Object[] {"abc", "abc"},
        new Object[] {"AbC", "abc"},
        new Object[] {"ABC", "ABC"},
        new Object[] {"ABC", "abc"},
        new Object[] {"abc", "acb"},
        new Object[] {"abc", "xyz"},
        new Object[] {"abc", "abcd"},
        new Object[] {"%abc", "%abc"},
        new Object[] {"%abc", "abc%"},
        new Object[] {"+inf", "+INF"},
        new Object[] {"-INF", "+INF"},
        new Object[] {"123abc!@#", "123ABC!@#"},
        new Object[] {"123abc!@#", "#@!cba321"},
        new Object[] {"abc", null}
    };
  }

  @SuppressWarnings("unused")
  private Object[] infinityStrings() {
    // string, isPositiveInfinity, isNegativeInfinity, isNaN
    return new Object[] {
        new Object[] {"inf", true, false, false},
        new Object[] {"+inf", true, false, false},
        new Object[] {"Infinity", true, false, false},
        new Object[] {"+Infinity", true, false, false},
        new Object[] {"-inf", false, true, false},
        new Object[] {"-Infinity", false, true, false},
        new Object[] {"infinite", false, false, false},
        new Object[] {"+infinityty", false, false, false},
        new Object[] {"+-inf", false, false, false},
        new Object[] {"notEvenClose", false, false, false},
        new Object[] {"NaN", false, false, true},
        new Object[] {null, false, false, false}
    };
  }

  @SuppressWarnings("unused")
  private Object[] infinityReturnStrings() {
    // string, expectedString
    return new Object[] {
        new Object[] {"inf", "inf"},
        new Object[] {"+inf", "inf"},
        new Object[] {"Infinity", "inf"},
        new Object[] {"+Infinity", "inf"},
        new Object[] {"-inf", "-inf"},
        new Object[] {"-Infinity", "-inf"},
    };
  }

}
