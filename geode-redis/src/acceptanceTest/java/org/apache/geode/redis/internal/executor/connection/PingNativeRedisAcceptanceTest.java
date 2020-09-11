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

package org.apache.geode.redis.internal.executor.connection;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import redis.clients.jedis.Jedis;

import org.apache.geode.NativeRedisTestRule;

public class PingNativeRedisAcceptanceTest extends PingIntegrationTest {

  @ClassRule
  public static NativeRedisTestRule redis = new NativeRedisTestRule();

  @BeforeClass
  public static void setUp() {
    jedis = new Jedis("localhost", redis.getPort(), REDIS_CLIENT_TIMEOUT);
  }
}
