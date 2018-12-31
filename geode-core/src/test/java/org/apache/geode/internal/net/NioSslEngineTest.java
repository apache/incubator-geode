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
package org.apache.geode.internal.net;

import static javax.net.ssl.SSLEngineResult.HandshakeStatus.FINISHED;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.NEED_TASK;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.NEED_UNWRAP;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.NEED_WRAP;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING;
import static javax.net.ssl.SSLEngineResult.Status.BUFFER_OVERFLOW;
import static javax.net.ssl.SSLEngineResult.Status.BUFFER_UNDERFLOW;
import static javax.net.ssl.SSLEngineResult.Status.CLOSED;
import static javax.net.ssl.SSLEngineResult.Status.OK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.Stack;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLSession;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.distributed.internal.DMStats;

public class NioSslEngineTest {
  static final int netBufferSize = 10000;
  static final int appBufferSize = 20000;

  SSLEngine mockEngine;
  DMStats mockStats;
  SSLSession mockSession;
  NioSslEngine nioSslEngine;
  SSLEngineResult mockEngineResult;
  NioSslEngine spyNioSslEngine;

  @Before
  public void setUp() throws Exception {
    mockEngine = mock(SSLEngine.class);

    mockSession = mock(SSLSession.class);
    when(mockEngine.getSession()).thenReturn(mockSession);
    when(mockSession.getPacketBufferSize()).thenReturn(netBufferSize);
    when(mockSession.getApplicationBufferSize()).thenReturn(appBufferSize);

    mockStats = mock(DMStats.class);

    nioSslEngine = new NioSslEngine(mockEngine, mockStats);
    spyNioSslEngine = spy(nioSslEngine);
  }

  @Test
  public void handshake() throws Exception {
    SocketChannel mockChannel = mock(SocketChannel.class);
    when(mockChannel.read(any(ByteBuffer.class))).thenReturn(100, 100, 100, 0);
    Socket mockSocket = mock(Socket.class);
    when(mockChannel.socket()).thenReturn(mockSocket);
    when(mockSocket.isClosed()).thenReturn(false);

    // initial read of handshake status followed by read of handshake status after task execution
    when(mockEngine.getHandshakeStatus()).thenReturn(NEED_UNWRAP, NEED_WRAP);

    // interleaved wraps/unwraps/task-execution
    when(mockEngine.unwrap(any(ByteBuffer.class), any(ByteBuffer.class))).thenReturn(
        new SSLEngineResult(OK, NEED_WRAP, 100, 100),
        new SSLEngineResult(BUFFER_OVERFLOW, NEED_UNWRAP, 0, 0),
        new SSLEngineResult(OK, NEED_TASK, 100, 0));

    when(mockEngine.getDelegatedTask()).thenReturn(new Runnable() {
      public void run() {}
    }, null);

    when(mockEngine.wrap(any(ByteBuffer.class), any(ByteBuffer.class))).thenReturn(
        new SSLEngineResult(OK, NEED_UNWRAP, 100, 100),
        new SSLEngineResult(BUFFER_OVERFLOW, NEED_WRAP, 0, 0),
        new SSLEngineResult(CLOSED, FINISHED, 100, 0));

    spyNioSslEngine.handshake(mockChannel, 10000, ByteBuffer.allocate(netBufferSize / 2));
    verify(mockEngine, times(2)).getHandshakeStatus();
    verify(mockEngine, times(3)).wrap(any(ByteBuffer.class), any(ByteBuffer.class));
    verify(mockEngine, times(3)).unwrap(any(ByteBuffer.class), any(ByteBuffer.class));
    verify(spyNioSslEngine, times(2)).expandBuffer(any(Buffers.BufferType.class),
        any(ByteBuffer.class), any(Integer.class), any(DMStats.class));
    verify(spyNioSslEngine, times(1)).handleBlockingTasks();
    verify(mockChannel, times(3)).read(any(ByteBuffer.class));
  }

  @Test
  public void handshakeUsesBufferParameter() throws Exception {
    SocketChannel mockChannel = mock(SocketChannel.class);
    when(mockChannel.read(any(ByteBuffer.class))).thenReturn(100, 100, 100, 0);
    Socket mockSocket = mock(Socket.class);
    when(mockChannel.socket()).thenReturn(mockSocket);
    when(mockSocket.isClosed()).thenReturn(false);

    // initial read of handshake status followed by read of handshake status after task execution
    when(mockEngine.getHandshakeStatus()).thenReturn(NEED_UNWRAP, NEED_WRAP);

    // interleaved wraps/unwraps/task-execution
    when(mockEngine.unwrap(any(ByteBuffer.class), any(ByteBuffer.class))).thenReturn(
        new SSLEngineResult(OK, NEED_WRAP, 100, 100),
        new SSLEngineResult(BUFFER_OVERFLOW, NEED_UNWRAP, 0, 0),
        new SSLEngineResult(OK, NEED_TASK, 100, 0));

    when(mockEngine.getDelegatedTask()).thenReturn(new Runnable() {
      public void run() {}
    }, null);

    when(mockEngine.wrap(any(ByteBuffer.class), any(ByteBuffer.class))).thenReturn(
        new SSLEngineResult(OK, NEED_UNWRAP, 100, 100),
        new SSLEngineResult(BUFFER_OVERFLOW, NEED_WRAP, 0, 0),
        new SSLEngineResult(CLOSED, FINISHED, 100, 0));

    ByteBuffer byteBuffer = ByteBuffer.allocate(netBufferSize);

    spyNioSslEngine.handshake(mockChannel, 10000, byteBuffer);

    assertThat(spyNioSslEngine.handshakeBuffer).isSameAs(byteBuffer);
  }


  @Test
  public void handshakeDetectsClosedSocket() throws Exception {
    SocketChannel mockChannel = mock(SocketChannel.class);
    when(mockChannel.read(any(ByteBuffer.class))).thenReturn(100, 100, 100, 0);
    Socket mockSocket = mock(Socket.class);
    when(mockChannel.socket()).thenReturn(mockSocket);
    when(mockSocket.isClosed()).thenReturn(true);

    // initial read of handshake status followed by read of handshake status after task execution
    when(mockEngine.getHandshakeStatus()).thenReturn(NEED_UNWRAP);

    ByteBuffer byteBuffer = ByteBuffer.allocate(netBufferSize);

    assertThatThrownBy(() -> spyNioSslEngine.handshake(mockChannel, 10000, byteBuffer))
        .isInstanceOf(
            SocketException.class)
        .hasMessageContaining("handshake terminated");
  }

  @Test
  public void handshakeDoesNotTerminateWithFinished() throws Exception {
    SocketChannel mockChannel = mock(SocketChannel.class);
    when(mockChannel.read(any(ByteBuffer.class))).thenReturn(100, 100, 100, 0);
    Socket mockSocket = mock(Socket.class);
    when(mockChannel.socket()).thenReturn(mockSocket);
    when(mockSocket.isClosed()).thenReturn(false);

    // initial read of handshake status followed by read of handshake status after task execution
    when(mockEngine.getHandshakeStatus()).thenReturn(NEED_UNWRAP);

    // interleaved wraps/unwraps/task-execution
    when(mockEngine.unwrap(any(ByteBuffer.class), any(ByteBuffer.class))).thenReturn(
        new SSLEngineResult(OK, NEED_WRAP, 100, 100));

    when(mockEngine.wrap(any(ByteBuffer.class), any(ByteBuffer.class))).thenReturn(
        new SSLEngineResult(CLOSED, NOT_HANDSHAKING, 100, 0));

    ByteBuffer byteBuffer = ByteBuffer.allocate(netBufferSize);

    assertThatThrownBy(() -> spyNioSslEngine.handshake(mockChannel, 10000, byteBuffer))
        .isInstanceOf(
            SSLHandshakeException.class)
        .hasMessageContaining("SSL Handshake terminated with status");
  }


  @Test
  public void checkClosed() {
    nioSslEngine.checkClosed();
  }

  @Test(expected = IllegalStateException.class)
  public void checkClosedThrows() throws Exception {
    when(mockEngine.wrap(any(ByteBuffer.class), any(ByteBuffer.class))).thenReturn(
        new SSLEngineResult(CLOSED, FINISHED, 0, 100));
    nioSslEngine.close(mock(SocketChannel.class));
    nioSslEngine.checkClosed();
  }

  @Test
  public void wrap() throws Exception {
    // make the application data too big to fit into the engine's encryption buffer
    ByteBuffer appData = ByteBuffer.allocate(nioSslEngine.myNetData.capacity() + 100);
    byte[] appBytes = new byte[appData.capacity()];
    Arrays.fill(appBytes, (byte) 0x1F);
    appData.put(appBytes);
    appData.flip();

    // create an engine that will transfer bytes from the application buffer to the encrypted buffer
    TestSSLEngine testEngine = new TestSSLEngine();
    testEngine.addReturnResult(
        new SSLEngineResult(OK, NEED_TASK, appData.remaining(), appData.remaining()));
    spyNioSslEngine.engine = testEngine;

    ByteBuffer wrappedBuffer = spyNioSslEngine.wrap(appData);

    verify(spyNioSslEngine, times(1)).expandBuffer(any(Buffers.BufferType.class),
        any(ByteBuffer.class), any(Integer.class), any(DMStats.class));
    appData.flip();
    assertThat(wrappedBuffer).isEqualTo(appData);
    verify(spyNioSslEngine, times(1)).handleBlockingTasks();
  }

  @Test
  public void wrapFails() throws Exception {
    // make the application data too big to fit into the engine's encryption buffer
    ByteBuffer appData = ByteBuffer.allocate(nioSslEngine.myNetData.capacity() + 100);
    byte[] appBytes = new byte[appData.capacity()];
    Arrays.fill(appBytes, (byte) 0x1F);
    appData.put(appBytes);
    appData.flip();

    // create an engine that will transfer bytes from the application buffer to the encrypted buffer
    TestSSLEngine testEngine = new TestSSLEngine();
    testEngine.addReturnResult(
        new SSLEngineResult(CLOSED, NEED_TASK, appData.remaining(), appData.remaining()));
    spyNioSslEngine.engine = testEngine;

    assertThatThrownBy(() -> spyNioSslEngine.wrap(appData)).isInstanceOf(SSLException.class)
        .hasMessageContaining("Error encrypting data");
  }

  @Test
  public void unwrap() throws Exception {
    // make the application data too big to fit into the engine's encryption buffer
    ByteBuffer wrappedData = ByteBuffer.allocate(nioSslEngine.peerAppData.capacity() + 100);
    byte[] netBytes = new byte[wrappedData.capacity()];
    Arrays.fill(netBytes, (byte) 0x1F);
    wrappedData.put(netBytes);
    wrappedData.flip();

    // create an engine that will transfer bytes from the application buffer to the encrypted buffer
    TestSSLEngine testEngine = new TestSSLEngine();
    spyNioSslEngine.engine = testEngine;

    testEngine.addReturnResult(new SSLEngineResult(OK, FINISHED, netBytes.length, netBytes.length));

    ByteBuffer unwrappedBuffer = spyNioSslEngine.unwrap(wrappedData);
    unwrappedBuffer.flip();

    verify(spyNioSslEngine, times(1)).expandBuffer(any(Buffers.BufferType.class),
        any(ByteBuffer.class), any(Integer.class), any(DMStats.class));
    assertThat(unwrappedBuffer).isEqualTo(ByteBuffer.wrap(netBytes));
  }

  @Test
  public void unwrapWithBufferUnderflow() throws Exception {
    // make the application data too big to fit into the engine's encryption buffer
    ByteBuffer wrappedData = ByteBuffer.allocate(nioSslEngine.peerAppData.capacity());
    byte[] netBytes = new byte[wrappedData.capacity() / 2];
    Arrays.fill(netBytes, (byte) 0x1F);
    wrappedData.put(netBytes);
    wrappedData.flip();

    // create an engine that will transfer bytes from the application buffer to the encrypted buffer
    TestSSLEngine testEngine = new TestSSLEngine();
    testEngine.addReturnResult(new SSLEngineResult(BUFFER_UNDERFLOW, NEED_TASK, 0, 0));
    spyNioSslEngine.engine = testEngine;

    ByteBuffer unwrappedBuffer = spyNioSslEngine.unwrap(wrappedData);
    unwrappedBuffer.flip();
    assertThat(unwrappedBuffer.remaining()).isEqualTo(0);
    assertThat(wrappedData.position()).isEqualTo(netBytes.length);
  }

  @Test
  public void getUnwrappedBuffer() {}

  @Test
  public void ensureUnwrappedCapacity() {}

  @Test
  public void close() {}

  @Test
  public void expandedCapacity() {}



  static class TestSSLEngine extends SSLEngine {

    private Stack<SSLEngineResult> returnResults = new Stack<>();

    @Override
    public SSLEngineResult wrap(ByteBuffer[] sources, int i, int i1, ByteBuffer destination)
        throws SSLException {
      for (ByteBuffer source : sources) {
        destination.put(source);
      }
      return returnResults.pop();
    }

    @Override
    public SSLEngineResult unwrap(ByteBuffer source, ByteBuffer[] destinations, int i, int i1)
        throws SSLException {
      SSLEngineResult sslEngineResult = returnResults.pop();
      if (sslEngineResult.getStatus() != BUFFER_UNDERFLOW) {
        destinations[0].put(source);
      }
      return sslEngineResult;
    }

    @Override
    public Runnable getDelegatedTask() {
      return null;
    }

    @Override
    public void closeInbound() throws SSLException {

    }

    @Override
    public boolean isInboundDone() {
      return false;
    }

    @Override
    public void closeOutbound() {

    }

    @Override
    public boolean isOutboundDone() {
      return false;
    }

    @Override
    public String[] getSupportedCipherSuites() {
      return new String[0];
    }

    @Override
    public String[] getEnabledCipherSuites() {
      return new String[0];
    }

    @Override
    public void setEnabledCipherSuites(String[] strings) {

    }

    @Override
    public String[] getSupportedProtocols() {
      return new String[0];
    }

    @Override
    public String[] getEnabledProtocols() {
      return new String[0];
    }

    @Override
    public void setEnabledProtocols(String[] strings) {

    }

    @Override
    public SSLSession getSession() {
      return null;
    }

    @Override
    public void beginHandshake() throws SSLException {

    }

    @Override
    public SSLEngineResult.HandshakeStatus getHandshakeStatus() {
      return null;
    }

    @Override
    public void setUseClientMode(boolean b) {

    }

    @Override
    public boolean getUseClientMode() {
      return false;
    }

    @Override
    public void setNeedClientAuth(boolean b) {

    }

    @Override
    public boolean getNeedClientAuth() {
      return false;
    }

    @Override
    public void setWantClientAuth(boolean b) {

    }

    @Override
    public boolean getWantClientAuth() {
      return false;
    }

    @Override
    public void setEnableSessionCreation(boolean b) {

    }

    @Override
    public boolean getEnableSessionCreation() {
      return false;
    }

    public void addReturnResult(SSLEngineResult sslEngineResult) {
      returnResults.add(sslEngineResult);
    }
  }
}
