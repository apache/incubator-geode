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

package org.apache.geode.internal.cache.tier.sockets;

import java.io.*;

import org.apache.geode.DataSerializer;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.tier.MessageType;

/**
 * Class <code>ClientMarkerMessageImpl</code> is a marker message that is placed in the
 * <code>CacheClientProxy</code>'s queue when the client connects to notify the client that all of
 * its queued updates have been sent. This is to be used mostly by the durable clients, although all
 * clients receive it.
 *
 *
 * @since GemFire 5.5
 */
public class ClientMarkerMessageImpl implements ClientMessage {
  private static final long serialVersionUID = 5423895238521508743L;

  /**
   * This <code>ClientMessage</code>'s <code>EventID</code>
   */
  private EventID eventId;

  /**
   * Constructor.
   *
   * @param eventId This <code>ClientMessage</code>'s <code>EventID</code>
   */
  public ClientMarkerMessageImpl(EventID eventId) {
    this.eventId = eventId;
  }

  /**
   * Default constructor.
   */
  public ClientMarkerMessageImpl() {}

  public MessageFromServer getMessage(CacheClientProxy proxy, boolean notify) throws IOException {
    Version clientVersion = proxy.getVersion();
    MessageFromServer message = null;
    if (clientVersion.compareTo(Version.GFE_57) >= 0) {
      message = getGFEMessage();
    } else {
      throw new IOException(
          "Unsupported client version for server-to-client message creation: " + clientVersion);
    }

    return message;
  }

  protected MessageFromServer getGFEMessage() throws IOException {
    MessageFromServer message = new MessageFromServer(1, Version.CURRENT);
    message.setMessageType(MessageType.CLIENT_MARKER);
    message.setTransactionId(0);
    message.addObjPart(this.eventId);
    return message;
  }

  public boolean shouldBeConflated() {
    return true;
  }

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeObject(this.eventId, out);
  }

  public int getDSFID() {
    return CLIENT_MARKER_MESSAGE_IMPL;
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.eventId = (EventID) DataSerializer.readObject(in);
  }

  public EventID getEventId() {
    return this.eventId;
  }

  public String getRegionToConflate() {
    return "gemfire_reserved_region_name_for_durable_client_marker";
  }

  public Object getKeyToConflate() {
    // This method can be called by HARegionQueue.
    // Use this to identify the message type.
    return "marker";
  }

  public Object getValueToConflate() {
    // This method can be called by HARegionQueue
    // Use this to identify the message type.
    return "marker";
  }

  public void setLatestValue(Object value) {
    return;
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }
}
