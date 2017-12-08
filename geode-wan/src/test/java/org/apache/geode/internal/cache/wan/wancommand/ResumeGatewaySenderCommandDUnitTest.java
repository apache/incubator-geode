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

package org.apache.geode.internal.cache.wan.wancommand;

import static org.apache.geode.distributed.ConfigurationProperties.DISTRIBUTED_SYSTEM_ID;
import static org.apache.geode.distributed.ConfigurationProperties.GROUPS;
import static org.apache.geode.distributed.ConfigurationProperties.REMOTE_LOCATORS;
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.createSender;
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.getMemberIdCallable;
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.pauseSender;
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.startSender;
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.verifySenderState;
import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;
import static org.apache.geode.test.dunit.Wait.pause;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;
import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category(DistributedTest.class)
public class ResumeGatewaySenderCommandDUnitTest {

  @Rule
  public LocatorServerStartupRule locatorServerStartupRule = new LocatorServerStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  private MemberVM locatorSite1;
  private MemberVM locatorSite2;
  private MemberVM server1;
  private MemberVM server2;
  private MemberVM server3;
  private MemberVM server4;
  private MemberVM server5;

  @Before
  public void before() throws Exception {
    Properties props = new Properties();
    props.setProperty(DISTRIBUTED_SYSTEM_ID, "" + 1);
    locatorSite1 = locatorServerStartupRule.startLocatorVM(1, props);

    props.setProperty(DISTRIBUTED_SYSTEM_ID, "" + 2);
    props.setProperty(REMOTE_LOCATORS, "localhost[" + locatorSite1.getPort() + "]");
    locatorSite2 = locatorServerStartupRule.startLocatorVM(2, props);

    // Connect Gfsh to locator.
    gfsh.connectAndVerify(locatorSite1);
  }

  @Test
  public void testResumeGatewaySender_ErrorConditions() throws Exception {

    Integer locator1Port = locatorSite1.getPort();

    // setup servers in Site #1
    server1 = locatorServerStartupRule.startServerVM(3, locator1Port);

    server1.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));

    final DistributedMember vm1Member = server1.invoke(getMemberIdCallable());
    String command = CliStrings.RESUME_GATEWAYSENDER + " --" + CliStrings.RESUME_GATEWAYSENDER__ID
        + "=ln --" + CliStrings.MEMBER + "=" + vm1Member.getId() + " --" + CliStrings.GROUP
        + "=SenderGroup1";
    gfsh.executeAndAssertThat(command).statusIsError()
        .containsOutput(CliStrings.PROVIDE_EITHER_MEMBER_OR_GROUP_MESSAGE);
  }

  @Test
  public void testResumeGatewaySender() throws Exception {

    Integer locator1Port = locatorSite1.getPort();

    // setup servers in Site #1
    server1 = locatorServerStartupRule.startServerVM(3, locator1Port);
    server2 = locatorServerStartupRule.startServerVM(4, locator1Port);
    server3 = locatorServerStartupRule.startServerVM(5, locator1Port);

    server1.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));
    server2.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));
    server3.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));

    server1.invoke(() -> startSender("ln"));
    server2.invoke(() -> startSender("ln"));
    server3.invoke(() -> startSender("ln"));

    server1.invoke(() -> verifySenderState("ln", true, false));
    server2.invoke(() -> verifySenderState("ln", true, false));
    server3.invoke(() -> verifySenderState("ln", true, false));

    server1.invoke(() -> pauseSender("ln"));
    server2.invoke(() -> pauseSender("ln"));
    server3.invoke(() -> pauseSender("ln"));

    server1.invoke(() -> verifySenderState("ln", true, true));
    server2.invoke(() -> verifySenderState("ln", true, true));
    server3.invoke(() -> verifySenderState("ln", true, true));

    pause(10000);
    String command =
        CliStrings.RESUME_GATEWAYSENDER + " --" + CliStrings.RESUME_GATEWAYSENDER__ID + "=ln";
    CommandResult cmdResult = gfsh.executeCommand(command);

    if (cmdResult != null) {
      String strCmdResult = cmdResult.toString();
      getLogWriter().info("testResumeGatewaySender stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      TabularResultData resultData = (TabularResultData) cmdResult.getResultData();
      List<String> status = resultData.retrieveAllValues("Result");
      assertEquals(3, status.size());
      assertTrue(status.contains("OK"));
    } else {
      fail("testResumeGatewaySender failed as did not get CommandResult");
    }
    server1.invoke(() -> verifySenderState("ln", true, false));
    server2.invoke(() -> verifySenderState("ln", true, false));
    server3.invoke(() -> verifySenderState("ln", true, false));
  }

  /**
   * test to validate that the start gateway sender starts the gateway sender on a member
   */
  @Test
  public void testResumeGatewaySender_onMember() throws Exception {
    Integer locator1Port = locatorSite1.getPort();

    // setup servers in Site #1
    server1 = locatorServerStartupRule.startServerVM(3, locator1Port);
    server2 = locatorServerStartupRule.startServerVM(4, locator1Port);
    server3 = locatorServerStartupRule.startServerVM(5, locator1Port);

    server1.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));
    server1.invoke(() -> startSender("ln"));
    server1.invoke(() -> verifySenderState("ln", true, false));
    server1.invoke(() -> pauseSender("ln"));
    server1.invoke(() -> verifySenderState("ln", true, true));

    final DistributedMember vm1Member = (DistributedMember) server1.invoke(getMemberIdCallable());
    pause(10000);
    String command = CliStrings.RESUME_GATEWAYSENDER + " --" + CliStrings.RESUME_GATEWAYSENDER__ID
        + "=ln --" + CliStrings.MEMBER + "=" + vm1Member.getId();
    CommandResult cmdResult = gfsh.executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = cmdResult.toString();
      getLogWriter().info("testResumeGatewaySender stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      assertTrue(strCmdResult.contains("is resumed on member"));
    } else {
      fail("testResumeGatewaySender failed as did not get CommandResult");
    }
    server1.invoke(() -> verifySenderState("ln", true, false));
  }

  /**
   * test to validate that the start gateway sender starts the gateway sender on a group of members
   */
  @Test
  public void testResumeGatewaySender_Group() throws Exception {

    Integer locator1Port = locatorSite1.getPort();

    // setup servers in Site #1
    /*
     * String group = "SenderGroup1"; Properties props = new Properties(); props.setProperty(GROUPS,
     * group); server1 = locatorServerStartupRule.startServerVM(3, props, locator1Port); server2 =
     * locatorServerStartupRule.startServerVM(4, props, locator1Port); server3 =
     * locatorServerStartupRule.startServerVM(5, props, locator1Port);
     */
    server1 = startServerWithGroups(3, "SenderGroup1", locator1Port);
    server2 = startServerWithGroups(4, "SenderGroup1", locator1Port);
    server3 = startServerWithGroups(5, "SenderGroup1", locator1Port);

    server1.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));
    server2.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));
    server3.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));

    server1.invoke(() -> startSender("ln"));
    server2.invoke(() -> startSender("ln"));
    server3.invoke(() -> startSender("ln"));

    server1.invoke(() -> verifySenderState("ln", true, false));
    server2.invoke(() -> verifySenderState("ln", true, false));
    server3.invoke(() -> verifySenderState("ln", true, false));

    server1.invoke(() -> pauseSender("ln"));
    server2.invoke(() -> pauseSender("ln"));
    server3.invoke(() -> pauseSender("ln"));

    server1.invoke(() -> verifySenderState("ln", true, true));
    server2.invoke(() -> verifySenderState("ln", true, true));
    server3.invoke(() -> verifySenderState("ln", true, true));

    pause(10000);
    String command = CliStrings.RESUME_GATEWAYSENDER + " --" + CliStrings.RESUME_GATEWAYSENDER__ID
        + "=ln --" + CliStrings.GROUP + "=SenderGroup1";
    CommandResult cmdResult = gfsh.executeCommand(command);

    if (cmdResult != null) {
      String strCmdResult = cmdResult.toString();
      getLogWriter().info("testResumeGatewaySender stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());

      TabularResultData resultData = (TabularResultData) cmdResult.getResultData();
      List<String> status = resultData.retrieveAllValues("Result");
      assertEquals(3, status.size());
      assertFalse(status.contains("Error"));
      assertTrue(status.contains("OK"));
    } else {
      fail("testResumeGatewaySender failed as did not get CommandResult");
    }
    server1.invoke(() -> verifySenderState("ln", true, false));
    server2.invoke(() -> verifySenderState("ln", true, false));
    server3.invoke(() -> verifySenderState("ln", true, false));
  }

  /**
   * Test to validate the scenario gateway sender is started when one or more sender members belongs
   * to multiple groups
   */
  @Test
  public void testResumeGatewaySender_MultipleGroup() throws Exception {

    Integer locator1Port = locatorSite1.getPort();

    server1 = startServerWithGroups(3, "SenderGroup1", locator1Port);
    server2 = startServerWithGroups(4, "SenderGroup1", locator1Port);
    server3 = startServerWithGroups(5, "SenderGroup1, SenderGroup2", locator1Port);
    server4 = startServerWithGroups(6, "SenderGroup2", locator1Port);
    server5 = startServerWithGroups(7, "SenderGroup3", locator1Port);

    server1.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));
    server2.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));
    server3.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));
    server4.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));
    server5.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));

    server1.invoke(() -> startSender("ln"));
    server2.invoke(() -> startSender("ln"));
    server3.invoke(() -> startSender("ln"));
    server4.invoke(() -> startSender("ln"));
    server5.invoke(() -> startSender("ln"));

    server1.invoke(() -> verifySenderState("ln", true, false));
    server2.invoke(() -> verifySenderState("ln", true, false));
    server3.invoke(() -> verifySenderState("ln", true, false));
    server4.invoke(() -> verifySenderState("ln", true, false));
    server5.invoke(() -> verifySenderState("ln", true, false));

    server1.invoke(() -> pauseSender("ln"));
    server2.invoke(() -> pauseSender("ln"));
    server3.invoke(() -> pauseSender("ln"));
    server4.invoke(() -> pauseSender("ln"));
    server5.invoke(() -> pauseSender("ln"));

    server1.invoke(() -> verifySenderState("ln", true, true));
    server2.invoke(() -> verifySenderState("ln", true, true));
    server3.invoke(() -> verifySenderState("ln", true, true));
    server4.invoke(() -> verifySenderState("ln", true, true));
    server5.invoke(() -> verifySenderState("ln", true, true));

    pause(10000);
    String command = CliStrings.RESUME_GATEWAYSENDER + " --" + CliStrings.RESUME_GATEWAYSENDER__ID
        + "=ln --" + CliStrings.GROUP + "=SenderGroup1,SenderGroup2";
    CommandResult cmdResult = gfsh.executeCommand(command);

    if (cmdResult != null) {
      String strCmdResult = cmdResult.toString();
      getLogWriter().info("testResumeGatewaySender stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      TabularResultData resultData = (TabularResultData) cmdResult.getResultData();
      List<String> status = resultData.retrieveAllValues("Result");
      assertEquals(4, status.size());
      assertFalse(status.contains("Error"));
      assertTrue(status.contains("OK"));
    } else {
      fail("testResumeGatewaySender failed as did not get CommandResult");
    }
    server1.invoke(() -> verifySenderState("ln", true, false));
    server2.invoke(() -> verifySenderState("ln", true, false));
    server3.invoke(() -> verifySenderState("ln", true, false));
    server4.invoke(() -> verifySenderState("ln", true, false));
    server5.invoke(() -> verifySenderState("ln", true, true));
  }

  private MemberVM startServerWithGroups(int index, String groups, int locPort) throws Exception {
    Properties props = new Properties();
    props.setProperty(GROUPS, groups);
    return locatorServerStartupRule.startServerVM(index, props, locPort);
  }
}
