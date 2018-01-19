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
package org.apache.geode.management.internal.cli.commands;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.AcceptanceTest;
import org.apache.geode.test.junit.rules.gfsh.GfshExecution;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;
import org.apache.geode.test.junit.rules.gfsh.GfshScript;

@Category(AcceptanceTest.class)
public class StartLocatorAcceptanceTest {
  @Rule
  public GfshRule gfshRule = new GfshRule();

  @Test
  public void startLocatorWithAutoConnectShouldBeConnectedAndRetrieveClusterConfigurationStatus()
      throws Exception {
    GfshExecution execution = GfshScript.of("start locator --name=locator1").execute(gfshRule);
    assertThat(execution.getOutputText()).contains("Successfully connected to: JMX Manager");
    assertThat(execution.getOutputText())
        .contains("Cluster configuration service is up and running.");
  }

  @Test
  public void startLocatorWithConnectFalseShouldNotBeConnected() throws Exception {
    GfshExecution execution =
        GfshScript.of("start locator --name=locator1 --connect=false").execute(gfshRule);
    assertThat(execution.getOutputText()).doesNotContain("Successfully connected to: JMX Manager");
  }

  @Test
  public void startLocatorWithConnectFalseShouldNotRetrieveClusterConfigurationStatus()
      throws Exception {
    GfshExecution execution =
        GfshScript.of("start locator --name=locator1 --connect=false").execute(gfshRule);
    assertThat(execution.getOutputText())
        .doesNotContain("Cluster configuration service is up and running.");
  }
}
