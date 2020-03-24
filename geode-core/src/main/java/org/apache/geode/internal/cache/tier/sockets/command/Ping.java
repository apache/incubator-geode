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
package org.apache.geode.internal.cache.tier.sockets.command;

import java.io.IOException;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.internal.cache.DistributedPingMessage;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.BaseCommand;
import org.apache.geode.internal.cache.tier.sockets.ClientHealthMonitor;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.security.SecurityService;

public class Ping extends BaseCommand {

  @Immutable
  private static final Ping singleton = new Ping();

  public static Command getCommand() {
    return singleton;
  }

  private Ping() {}

  @Override
  public void cmdExecute(final Message clientMessage, final ServerConnection serverConnection,
      final SecurityService securityService, long start) throws IOException {
    final boolean isDebugEnabled = logger.isDebugEnabled();
    if (isDebugEnabled) {
      logger.debug("{}: rcv tx: {} from {} rcvTime: {}", serverConnection.getName(),
          clientMessage.getTransactionId(), serverConnection.getSocketString(),
          (DistributionStats.getStatTime() - start));
    }
    if (clientMessage.getNumberOfParts() > 0) {
      try {
        DistributedMember targetServer = (DistributedMember) clientMessage.getPart(0).getObject();
        DistributedMember myID = serverConnection.getCache().getMyId();
        if (!myID.equals(targetServer)) {
          pingCorrectServer(clientMessage, targetServer, serverConnection);
          writeReply(clientMessage, serverConnection);
          serverConnection.setAsTrue(RESPONDED);
          return;
        }
      } catch (ClassNotFoundException e) {
        logger.warn("Unable to deserialize message from " + serverConnection.getProxyID());
        writeErrorResponse(clientMessage, MessageType.PING, serverConnection);
        serverConnection.setAsTrue(RESPONDED);
        return;
      }
    }
    ClientHealthMonitor chm = ClientHealthMonitor.getInstance();
    if (chm != null) {
      chm.receivedPing(serverConnection.getProxyID());
    }
    writeReply(clientMessage, serverConnection);
    serverConnection.setAsTrue(RESPONDED);
    if (isDebugEnabled) {
      logger.debug("{}: Sent ping reply to {}", serverConnection.getName(),
          serverConnection.getSocketString());
    }
  }

  /**
   * Process a ping request that was sent to the wrong server
   */
  protected void pingCorrectServer(Message clientMessage, DistributedMember targetServer,
      ServerConnection serverConnection)
      throws IOException {
    if (logger.isDebugEnabled()) {
      logger.debug("Received a Ping request from {} intended for {}.  Forwarding the ping...");
    }
    if (!serverConnection.getCache().getDistributionManager().isCurrentMember(targetServer)) {
      logger.warn("Unable to ping non-member {} for client {}", targetServer,
          serverConnection.getProxyID());
      writeErrorResponse(clientMessage, MessageType.PING, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
    } else {
      // send a ping message to the server. This is a one-way message that doesn't send a reply
      final DistributedPingMessage distributedPingMessage =
          new DistributedPingMessage(targetServer, serverConnection.getProxyID());
      serverConnection.getCache().getDistributionManager().putOutgoing(distributedPingMessage);
      writeReply(clientMessage, serverConnection);
    }
  }

  @Override
  protected void writeReply(Message origMsg, ServerConnection serverConnection) throws IOException {
    Message replyMsg = serverConnection.getReplyMessage();
    serverConnection.getCache().getCancelCriterion().checkCancelInProgress(null);
    replyMsg.setMessageType(MessageType.REPLY);
    replyMsg.setNumberOfParts(1);
    replyMsg.setTransactionId(origMsg.getTransactionId());
    replyMsg.addBytesPart(okBytes());
    replyMsg.send(serverConnection);
    if (logger.isTraceEnabled()) {
      logger.trace("{}: rpl tx: {}", serverConnection.getName(), origMsg.getTransactionId());
    }
  }
}
