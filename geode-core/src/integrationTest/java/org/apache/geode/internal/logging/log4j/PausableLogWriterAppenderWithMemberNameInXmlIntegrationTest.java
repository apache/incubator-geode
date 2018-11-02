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

import static java.nio.charset.Charset.defaultCharset;
import static org.apache.commons.io.FileUtils.readFileToString;
import static org.apache.commons.io.FileUtils.readLines;
import static org.apache.commons.lang.SystemUtils.LINE_SEPARATOR;
import static org.apache.geode.internal.logging.LogMessageRegex.Group;
import static org.apache.geode.internal.logging.LogMessageRegex.getPattern;
import static org.apache.geode.internal.logging.NonBlankStrings.nonBlankStrings;
import static org.apache.geode.test.util.ResourceUtils.createFileFromResource;
import static org.apache.geode.test.util.ResourceUtils.getResource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.net.URL;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.junit.LoggerContextRule;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.internal.logging.LogConfig;
import org.apache.geode.internal.logging.LogConfigSupplier;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.SessionContext;
import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Integration tests for {@link PausableLogWriterAppender} with {@code memberName} in
 * {@code log4j2.xml}.
 */
@Category(LoggingTest.class)
public class PausableLogWriterAppenderWithMemberNameInXmlIntegrationTest {

  private static final String CONFIG_FILE_NAME =
      "PausableLogWriterAppenderWithMemberNameInXmlIntegrationTest_log4j2.xml";
  private static final String APPENDER_NAME = "LOGWRITERWITHMEMBERNAME";
  private static final String MEMBER_NAME = "MEMBERNAME";

  private static String configFilePath;

  private PausableLogWriterAppender pausableLogWriterAppender;
  private File logFile;
  private Logger logger;
  private String logMessage;

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public LoggerContextRule loggerContextRule = new LoggerContextRule(configFilePath);

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
    String logFileName = MEMBER_NAME + ".log";
    logFile = new File(temporaryFolder.newFolder(testName.getMethodName()), logFileName);

    LogConfig config = mock(LogConfig.class);
    when(config.getName()).thenReturn("");
    when(config.getLogFile()).thenReturn(logFile);

    LogConfigSupplier logConfigSupplier = mock(LogConfigSupplier.class);
    when(logConfigSupplier.getLogConfig()).thenReturn(config);

    SessionContext sessionContext = mock(SessionContext.class);
    when(sessionContext.getLogConfigSupplier()).thenReturn(logConfigSupplier);

    pausableLogWriterAppender =
        loggerContextRule.getAppender(APPENDER_NAME, PausableLogWriterAppender.class);
    pausableLogWriterAppender.createSession(sessionContext);
    pausableLogWriterAppender.startSession();

    logger = LogService.getLogger();
    logMessage = "Logging in " + testName.getMethodName();
  }

  @After
  public void tearDown() {
    pausableLogWriterAppender.stopSession();
  }

  @Test
  public void logsToSpecifiedFile() throws Exception {
    logger.info(logMessage);

    assertThat(logFile).exists();
    String content = readFileToString(logFile, defaultCharset()).trim();
    assertThat(content).contains(logMessage);
  }

  @Test
  public void logLinesInFileShouldContainMemberName() throws Exception {
    logger.info(logMessage);

    assertThat(logFile).exists();

    List<String> lines = nonBlankStrings(readLines(logFile, defaultCharset()));
    assertThat(lines).hasSize(1);

    for (String line : lines) {
      Matcher matcher = getPattern().matcher(line);
      assertThat(matcher.matches()).as(failedToMatchRegex(line, getPattern())).isTrue();
      assertThat(matcher.group(Group.MEMBER_NAME.getName())).isEqualTo(MEMBER_NAME);
    }

  }

  private String failedToMatchRegex(String line, Pattern pattern) {
    String $ = LINE_SEPARATOR;
    return $ + "Line:" + $ + " " + line + $ + "failed to match regex:" + $ + " " + pattern + $;
  }
}
