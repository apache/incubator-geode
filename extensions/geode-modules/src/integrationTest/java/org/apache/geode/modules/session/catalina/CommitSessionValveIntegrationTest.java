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
package org.apache.geode.modules.session.catalina;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import javax.servlet.ServletException;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.catalina.Context;
import org.apache.catalina.Manager;
import org.apache.catalina.connector.Request;
import org.apache.catalina.connector.Response;
import org.apache.juli.logging.Log;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.cache.RegionShortcut;

@RunWith(JUnitParamsRunner.class)
public class CommitSessionValveIntegrationTest extends AbstractSessionValveIntegrationTest {
  private Request request;
  private Response response;
  private TestValve testValve;
  private CommitSessionValve commitSessionValve;
  private DeltaSessionFacade deltaSessionFacade;

  @Before
  public void setUp() {
    request = spy(Request.class);
    response = spy(Response.class);
    testValve = new TestValve(false);

    commitSessionValve = new CommitSessionValve();
    commitSessionValve.setNext(testValve);
  }

  protected void parameterizedSetUp(RegionShortcut regionShortcut) {
    super.parameterizedSetUp(regionShortcut);

    deltaSessionFacade = new DeltaSessionFacade(deltaSession);
    when(request.getContext()).thenReturn(mock(Context.class));

    // Valve use the context to log messages
    when(deltaSessionManager.getTheContext()).thenReturn(mock(Context.class));
    when(deltaSessionManager.getTheContext().getLogger()).thenReturn(mock(Log.class));
  }

  @Test
  @Parameters({"REPLICATE", "PARTITION"})
  public void invokeShouldCallNextChainedValveAndDoNothingWhenSessionManagerDoesNotBelongToGeode(
      RegionShortcut regionShortcut) throws IOException, ServletException {
    parameterizedSetUp(regionShortcut);
    when(request.getContext().getManager()).thenReturn(mock(Manager.class));

    commitSessionValve.invoke(request, response);
    assertThat(testValve.invocations.get()).isEqualTo(1);
    verify(request, times(0)).getSession();
  }

  @Test
  @Parameters({"REPLICATE", "PARTITION"})
  public void invokeShouldCallNextChainedValveAndDoNothingWhenSessionManagerBelongsToGeodeButSessionDoesNotExist(
      RegionShortcut regionShortcut) throws IOException, ServletException {
    parameterizedSetUp(regionShortcut);
    doReturn(null).when(request).getSession(false);
    when(request.getContext().getManager()).thenReturn(deltaSessionManager);

    commitSessionValve.invoke(request, response);
    assertThat(testValve.invocations.get()).isEqualTo(1);
    verify(request, times(1)).getSession(false);
    verify(deltaSessionManager, times(0)).removeTouchedSession(anyString());
  }

  @Test
  @Parameters({"REPLICATE", "PARTITION"})
  public void invokeShouldCallNextChainedValveAndDoNothingWhenSessionManagerBelongsToGeodeAndSessionExistsButIsNotValid(
      RegionShortcut regionShortcut) throws IOException, ServletException {
    parameterizedSetUp(regionShortcut);
    doReturn(false).when(deltaSession).isValid();
    doReturn(deltaSessionFacade).when(request).getSession(false);
    when(request.getContext().getManager()).thenReturn(deltaSessionManager);

    commitSessionValve.invoke(request, response);
    assertThat(testValve.invocations.get()).isEqualTo(1);
    verify(request, times(1)).getSession(false);
    verify(deltaSessionManager, times(0)).removeTouchedSession(anyString());
  }

  @Test
  @Parameters({"REPLICATE", "PARTITION"})
  public void invokeShouldCallNextChainedValveAndCommitTheExistingValidSessionWhenSessionManagerBelongsToGeode(
      RegionShortcut regionShortcut) throws IOException, ServletException {
    parameterizedSetUp(regionShortcut);
    deltaSessionManager.addSessionToTouch(TEST_SESSION_ID);
    doReturn(deltaSessionFacade).when(request).getSession(false);
    when(request.getContext().getManager()).thenReturn(deltaSessionManager);

    commitSessionValve.invoke(request, response);
    assertThat(testValve.invocations.get()).isEqualTo(1);
    assertThat(httpSessionRegion.containsKey(TEST_SESSION_ID)).isTrue();
    assertThat(deltaSessionManager.getSessionsToTouch().contains(TEST_SESSION_ID)).isFalse();
  }

  @Test
  @Parameters({"REPLICATE", "PARTITION"})
  public void invokeShouldCommitTheExistingValidSessionWhenSessionManagerBelongsToGeodeEvenWhenTheNextChainedValveThrowsAnException(
      RegionShortcut regionShortcut) {
    parameterizedSetUp(regionShortcut);
    TestValve exceptionValve = new TestValve(true);
    commitSessionValve.setNext(exceptionValve);
    deltaSessionManager.addSessionToTouch(TEST_SESSION_ID);
    doReturn(deltaSessionFacade).when(request).getSession(false);
    when(request.getContext().getManager()).thenReturn(deltaSessionManager);

    assertThatThrownBy(() -> commitSessionValve.invoke(request, response))
        .isInstanceOf(RuntimeException.class);
    assertThat(exceptionValve.invocations.get()).isEqualTo(1);
    assertThat(exceptionValve.exceptionsThrown.get()).isEqualTo(1);
    assertThat(httpSessionRegion.containsKey(TEST_SESSION_ID)).isTrue();
    assertThat(deltaSessionManager.getSessionsToTouch().contains(TEST_SESSION_ID)).isFalse();
  }
}
