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
package org.apache.geode.internal.alerting;

import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.distributed.ConfigurationProperties.START_LOCATOR;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPort;
import static org.apache.geode.internal.admin.remote.AlertListenerMessage.addListener;
import static org.apache.geode.internal.admin.remote.AlertListenerMessage.removeListener;
import static org.apache.geode.internal.alerting.AlertLevel.ERROR;
import static org.apache.geode.internal.alerting.AlertLevel.NONE;
import static org.apache.geode.internal.alerting.AlertLevel.SEVERE;
import static org.apache.geode.internal.alerting.AlertLevel.WARNING;
import static org.apache.geode.test.awaitility.GeodeAwaitility.getTimeout;
import static org.apache.geode.test.dunit.NetworkUtils.getServerHostName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.Properties;

import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mockito.ArgumentCaptor;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.admin.remote.AlertListenerMessage;
import org.apache.geode.internal.admin.remote.AlertListenerMessage.Listener;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.internal.AlertDetails;
import org.apache.geode.test.junit.categories.AlertingTest;

@Category(AlertingTest.class)
public class DistributedSystemAlertingIntegrationTest {

  private static final long TIMEOUT = getTimeout().getValueInMS();

  private InternalDistributedSystem system;
  private DistributedMember member;
  private AlertingService alertingService;
  private Listener messageListener;
  private Logger logger;
  private String connectionName;
  private String alertMessage;
  private String exceptionMessage;
  private String threadName;
  private long threadId;

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() {
    alertMessage = "Alerting in " + testName.getMethodName();
    exceptionMessage = "Exception in " + testName.getMethodName();
    connectionName = "Member in " + testName.getMethodName();
    threadName = Thread.currentThread().getName();
    threadId = Long.valueOf(Long.toHexString(Thread.currentThread().getId()));

    String startLocator = getServerHostName() + "[" + getRandomAvailableTCPPort() + "]";

    Properties config = new Properties();
    config.setProperty(START_LOCATOR, startLocator);
    config.setProperty(NAME, connectionName);

    system = (InternalDistributedSystem) DistributedSystem.connect(config);

    member = system.getDistributedMember();
    alertingService = system.getAlertingService();
    messageListener = spy(Listener.class);
    addListener(messageListener);
    logger = LogService.getLogger();
  }

  @After
  public void tearDown() {
    removeListener(messageListener);
    system.disconnect();
  }

  @Test
  public void alertMessageIsNotReceivedByDefault() {
    logger.warn(alertMessage);

    verifyNoMoreInteractions(messageListener);
  }

  @Test
  public void alertMessageIsReceivedForLevelWarning() {
    alertingService.addAlertListener(member, WARNING);

    logger.warn(alertMessage);

    verify(messageListener, timeout(TIMEOUT)).received(isA(AlertListenerMessage.class));
  }

  @Test
  public void alertMessageIsReceivedForLevelError() {
    alertingService.addAlertListener(member, ERROR);

    logger.error(alertMessage);

    verify(messageListener, timeout(TIMEOUT)).received(isA(AlertListenerMessage.class));
  }

  @Test
  public void alertMessageIsReceivedForLevelFatal() {
    alertingService.addAlertListener(member, SEVERE);

    logger.fatal(alertMessage);

    verify(messageListener, timeout(TIMEOUT)).received(isA(AlertListenerMessage.class));
  }

  @Test
  public void alertMessageIsNotReceivedForLevelNone() {
    alertingService.addAlertListener(member, NONE);

    logger.fatal(alertMessage);

    verifyNoMoreInteractions(messageListener);
  }

  @Test
  public void alertMessageIsReceivedForHigherLevels() {
    alertingService.addAlertListener(member, WARNING);

    logger.error(alertMessage);
    logger.fatal(alertMessage);

    verify(messageListener, timeout(TIMEOUT).times(2)).received(isA(AlertListenerMessage.class));
  }

  @Test
  public void alertMessageIsNotReceivedForLowerLevels() {
    alertingService.addAlertListener(member, SEVERE);

    logger.warn(alertMessage);
    logger.error(alertMessage);

    verifyNoMoreInteractions(messageListener);
  }

  @Test
  public void alertDetailsIsCreatedByAlertMessage() {
    alertingService.addAlertListener(member, WARNING);

    logger.warn(alertMessage);

    assertThat(captureAlertDetails()).isNotNull().isInstanceOf(AlertDetails.class);
  }

  @Test
  public void alertDetailsAlertLevelMatches() {
    alertingService.addAlertListener(member, WARNING);

    logger.warn(alertMessage);

    assertThat(captureAlertDetails().getAlertLevel()).isEqualTo(WARNING.intLevel());
  }

  @Test
  public void alertDetailsMessageMatches() {
    alertingService.addAlertListener(member, WARNING);

    logger.warn(alertMessage);

    assertThat(captureAlertDetails().getMsg()).isEqualTo(alertMessage);
  }

  @Test
  public void alertDetailsSenderIsNullForLocalAlert() {
    alertingService.addAlertListener(member, WARNING);

    logger.warn(alertMessage);

    assertThat(captureAlertDetails().getSender()).isNull();
  }

  @Test
  public void alertDetailsSource() {
    alertingService.addAlertListener(member, WARNING);

    logger.warn(alertMessage);

    assertThat(captureAlertDetails().getSource()).contains(threadName);
  }

  @Test
  public void alertDetailsConnectionName() {
    alertingService.addAlertListener(member, WARNING);

    logger.warn(alertMessage);

    assertThat(captureAlertDetails().getConnectionName()).isEqualTo(connectionName);
  }

  @Test
  public void alertDetailsExceptionTextIsEmpty() {
    alertingService.addAlertListener(member, WARNING);

    logger.warn(alertMessage);

    assertThat(captureAlertDetails().getExceptionText()).isEqualTo("");
  }

  @Test
  public void alertDetailsExceptionTextMatches() {
    alertingService.addAlertListener(member, WARNING);

    logger.warn(alertMessage, new Exception(exceptionMessage));

    assertThat(captureAlertDetails().getExceptionText()).contains(exceptionMessage);
  }

  @Test
  public void alertDetailsThreadName() {
    alertingService.addAlertListener(member, WARNING);

    logger.warn(alertMessage);

    assertThat(captureAlertDetails().getThreadName()).isEqualTo(threadName);
  }

  @Test
  public void alertDetailsThreadId() {
    alertingService.addAlertListener(member, WARNING);

    logger.warn(alertMessage);

    assertThat(captureAlertDetails().getTid()).isEqualTo(threadId);
  }

  @Test
  public void alertDetailsMessageTime() {
    alertingService.addAlertListener(member, WARNING);

    logger.warn(alertMessage);

    assertThat(captureAlertDetails().getMsgTime()).isNotNull();
  }

  private AlertDetails captureAlertDetails() {
    ArgumentCaptor<AlertDetails> alertDetailsCaptor = ArgumentCaptor.forClass(AlertDetails.class);
    verify(messageListener, timeout(TIMEOUT)).created(alertDetailsCaptor.capture());
    return alertDetailsCaptor.getValue();
  }
}
