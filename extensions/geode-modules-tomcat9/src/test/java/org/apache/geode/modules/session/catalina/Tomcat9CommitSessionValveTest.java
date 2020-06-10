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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.catalina.Context;
import org.apache.catalina.connector.Request;
import org.apache.catalina.connector.Response;
import org.apache.coyote.OutputBuffer;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;


public class Tomcat9CommitSessionValveTest {

  @Test
  public void test() throws IOException {
    final Context context = mock(Context.class);

    final Request request = mock(Request.class);
    doReturn(context).when(request).getContext();

    final OutputBuffer outputBuffer = mock(OutputBuffer.class);

    final org.apache.coyote.Response coyoteResponse = new org.apache.coyote.Response();
    coyoteResponse.setOutputBuffer(outputBuffer);

    final Response response = new Response();
    response.setRequest(request);
    response.setCoyoteResponse(coyoteResponse);

    final Tomcat9CommitSessionValve valve = new Tomcat9CommitSessionValve();
    final OutputStream outputStream = valve.wrapResponse(response).getResponse().getOutputStream();
    outputStream.write(new byte[] {'a', 'b', 'c'});
    outputStream.flush();

    final ArgumentCaptor<ByteBuffer> byteBuffer = ArgumentCaptor.forClass(ByteBuffer.class);

    final InOrder inOrder = inOrder(outputBuffer);
    inOrder.verify(outputBuffer).doWrite(byteBuffer.capture());
    inOrder.verifyNoMoreInteractions();

    assertThat(byteBuffer.getValue().array()).contains('a', 'b', 'c');
  }

}
