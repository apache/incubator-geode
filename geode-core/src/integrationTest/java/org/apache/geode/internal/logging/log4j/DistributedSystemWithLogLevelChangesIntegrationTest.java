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
package org.apache.geode.internal.logging.log4j;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.internal.logging.Configuration.DEFAULT_LOGWRITER_LEVEL;
import static org.apache.geode.internal.logging.Configuration.LOG_LEVEL_UPDATE_OCCURS_PROPERTY;
import static org.apache.geode.internal.logging.InternalLogWriter.FINE_LEVEL;
import static org.apache.geode.internal.logging.InternalLogWriter.WARNING_LEVEL;
import static org.apache.geode.test.util.ResourceUtils.createFileFromResource;
import static org.apache.geode.test.util.ResourceUtils.getResource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.net.URL;
import java.util.List;
import java.util.Properties;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.junit.LoggerContextRule;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.logging.Configuration.LogLevelUpdateOccurs;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Integration tests for {@link DistributedSystem} and log level changes with
 * {@link PausableConsoleAppender}.
 */
@Category(LoggingTest.class)
public class DistributedSystemWithLogLevelChangesIntegrationTest {

  private static final String CONFIG_FILE_NAME =
      "DistributedSystemWithLogLevelChangesIntegrationTest_log4j2.xml";
  private static final String APPENDER_NAME = "STDOUT";
  private static final String APPLICATION_LOGGER_NAME = "com.company.application";

  private static String configFilePath;

  private InternalDistributedSystem system;

  private PausableConsoleAppender pausableConsoleAppender;
  private DistributionConfig distributionConfig;
  private Logger geodeLogger;
  private Logger applicationLogger;
  private String logMessage;

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public LoggerContextRule loggerContextRule = new LoggerContextRule(configFilePath);

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Rule
  public TestName testName = new TestName();

  @BeforeClass
  public static void setUpLogConfigFile() throws Exception {
    URL resource = getResource(CONFIG_FILE_NAME);
    configFilePath = createFileFromResource(resource, temporaryFolder.getRoot(), CONFIG_FILE_NAME)
        .getAbsolutePath();
  }

  @Before
  public void setUp() throws Exception {
    pausableConsoleAppender =
        loggerContextRule.getAppender(APPENDER_NAME, PausableConsoleAppender.class);
    assertThat(pausableConsoleAppender).isNotNull();

    System.setProperty(LOG_LEVEL_UPDATE_OCCURS_PROPERTY, LogLevelUpdateOccurs.ALWAYS.name());

    Properties config = new Properties();
    config.setProperty(LOCATORS, "");
    system = (InternalDistributedSystem) DistributedSystem.connect(config);

    distributionConfig = system.getConfig();

    geodeLogger = LogService.getLogger();
    applicationLogger = LogService.getLogger(APPLICATION_LOGGER_NAME);

    logMessage = "Logging in " + testName.getMethodName();
  }

  @After
  public void tearDown() throws Exception {
    system.disconnect();
    pausableConsoleAppender.clearLogEvents();
  }

  @Test
  public void debugNotLoggedByDefault() {
    geodeLogger.debug(logMessage);

    assertThatLogEventsDoesNotContain(logMessage, getClass().getName(), Level.DEBUG);
  }

  @Test
  public void debugLoggedAfterLoweringLogLevelToFine() {
    distributionConfig.setLogLevel(FINE_LEVEL);

    geodeLogger.debug(logMessage);

    assertThatLogEventsContains(logMessage, geodeLogger.getName(), Level.DEBUG);
  }

  @Test
  public void debugNotLoggedAfterRestoringLogLevelToDefault() {
    distributionConfig.setLogLevel(FINE_LEVEL);

    system.getConfig().setLogLevel(DEFAULT_LOGWRITER_LEVEL);
    geodeLogger.debug(logMessage);

    assertThatLogEventsDoesNotContain(logMessage, geodeLogger.getName(), Level.DEBUG);
  }

  @Test
  public void applicationLoggerInfoLoggedByDefault() {
    applicationLogger.info(logMessage);

    assertThatLogEventsContains(logMessage, applicationLogger.getName(), Level.INFO);
  }

  @Test
  public void applicationLoggerBelowLevelUnaffectedByLoweringLogLevelChanges() {
    distributionConfig.setLogLevel(FINE_LEVEL);

    applicationLogger.debug(logMessage);

    assertThatLogEventsDoesNotContain(logMessage, applicationLogger.getName(), Level.DEBUG);
  }

  @Test
  public void applicationLoggerAboveLevelUnaffectedByLoweringLogLevelChanges() {
    distributionConfig.setLogLevel(FINE_LEVEL);

    applicationLogger.info(logMessage);

    assertThatLogEventsContains(logMessage, applicationLogger.getName(), Level.INFO);
  }

  @Test
  public void applicationLoggerAboveLevelUnaffectedByRaisingLogLevelChanges() {
    distributionConfig.setLogLevel(WARNING_LEVEL);

    applicationLogger.info(logMessage);

    assertThatLogEventsContains(logMessage, applicationLogger.getName(), Level.INFO);
  }

  @Test
  public void infoStatementNotLoggedAfterRaisingLogLevelToWarning() {
    distributionConfig.setLogLevel(WARNING_LEVEL);

    geodeLogger.info(logMessage);

    assertThatLogEventsDoesNotContain(logMessage, geodeLogger.getName(), Level.INFO);
  }

  private void assertThatLogEventsContains(String message, String loggerName, Level level) {
    List<LogEvent> logEvents = pausableConsoleAppender.getLogEvents();
    for (LogEvent event : logEvents) {
      if (event.getMessage().getFormattedMessage().contains(message)) {
        assertThat(event.getMessage().getFormattedMessage()).isEqualTo(message);
        assertThat(event.getLoggerName()).isEqualTo(loggerName);
        assertThat(event.getLevel()).isEqualTo(level);
        return;
      }
    }
    fail("Expected message " + message + " not found in " + logEvents);
  }

  private void assertThatLogEventsDoesNotContain(String message, String loggerName, Level level) {
    List<LogEvent> logEvents = pausableConsoleAppender.getLogEvents();
    for (LogEvent event : logEvents) {
      if (event.getMessage().getFormattedMessage().contains(message) &&
          event.getLoggerName().equals(loggerName) && event.getLevel().equals(level)) {
        fail("Expected message " + message + " should not be contained in " + logEvents);
      }
    }
  }
}
