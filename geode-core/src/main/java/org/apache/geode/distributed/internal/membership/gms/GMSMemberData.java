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
package org.apache.geode.distributed.internal.membership.gms;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;

import org.jgroups.util.UUID;

import org.apache.geode.distributed.internal.membership.api.MemberData;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.serialization.StaticSerialization;
import org.apache.geode.internal.serialization.Version;

/**
 * GMSMember contains data that is required to identify a member of the cluster.
 * Unfortunately it is also used in identifying client caches in a client/server
 * configuration and so contains weird things like a durable-id and a durable-timeout.
 */
public class GMSMemberData implements MemberData, Comparable<GMSMemberData> {
  /** The type for regular members */
  public static final int NORMAL_DM_TYPE = 10;

  /** The DM type for locator members */
  public static final int LOCATOR_DM_TYPE = 11;

  /** The DM type for deprecated admin-only members */
  public static final int ADMIN_ONLY_DM_TYPE = 12;

  /** The DM type for stand-alone members (usually clients) */
  public static final int LONER_DM_TYPE = 13;

  private String hostName;

  private int udpPort = 0;
  private boolean preferredForCoordinator;
  private boolean networkPartitionDetectionEnabled;
  private byte memberWeight;
  private InetAddress inetAddr;
  private int processId;
  private byte vmKind;
  private int vmViewId = -1;
  private int directPort;
  private String name;
  private String[] groups;
  private short versionOrdinal = Version.getCurrentVersion().ordinal();
  private long uuidLSBs;
  private long uuidMSBs;
  private String durableId;
  private int durableTimeout;

  private boolean isPartial; // transient state - created with readEssentialData

  public boolean isPartial() {
    return isPartial;
  }

  public GMSMemberData() {}

  /**
   * Create a CacheMember referring to the current host (as defined by the given string).
   *
   * @param i the hostname, must be for the current host
   * @param membershipPort the membership listening port
   * @param networkPartitionDetectionEnabled whether the member has network partition detection
   *        enabled
   * @param preferredForCoordinator whether the member can be group coordinator
   * @param version the member's version ordinal
   * @param msbs - most significant bytes of UUID
   * @param lsbs - least significant bytes of UUID
   */
  public GMSMemberData(InetAddress i, String hostName, int membershipPort, int processId,
      byte vmKind,
      int directPort, int vmViewId,
      String name, String[] groups,
      String durableId, int durableTimeout,
      boolean networkPartitionDetectionEnabled, boolean preferredForCoordinator,
      short version,
      long msbs, long lsbs, byte memberWeight) {
    this.inetAddr = i;
    this.hostName = hostName;
    this.udpPort = membershipPort;
    this.processId = processId;
    this.vmKind = vmKind;
    this.directPort = directPort;
    this.vmViewId = vmViewId;
    this.name = name;
    this.groups = groups;
    this.durableId = durableId;
    this.durableTimeout = durableTimeout;
    this.networkPartitionDetectionEnabled = networkPartitionDetectionEnabled;
    this.preferredForCoordinator = preferredForCoordinator;
    this.versionOrdinal = version;
    this.uuidMSBs = msbs;
    this.uuidLSBs = lsbs;
    this.memberWeight = memberWeight;
  }

  public GMSMemberData(InetAddress i, int p, short version, long msbs, long lsbs, int viewId) {
    this.inetAddr = i;
    this.hostName = i.getHostName();
    this.udpPort = p;
    this.versionOrdinal = version;
    this.uuidMSBs = msbs;
    this.uuidLSBs = lsbs;
    this.vmViewId = viewId;
  }


  /**
   * Clone a GMSMemberInfo
   *
   * @param other the member to create a copy of
   */
  public GMSMemberData(GMSMemberData other) {
    this.hostName = other.hostName;
    this.udpPort = other.udpPort;
    this.preferredForCoordinator = other.preferredForCoordinator;
    this.networkPartitionDetectionEnabled = other.networkPartitionDetectionEnabled;
    this.memberWeight = other.memberWeight;
    this.inetAddr = other.inetAddr;
    this.processId = other.processId;
    this.vmKind = other.vmKind;
    this.vmViewId = other.vmViewId;
    this.directPort = other.directPort;
    this.name = other.name;
    this.durableId = other.durableId;
    this.durableTimeout = other.durableTimeout;
    this.groups = other.groups;
    this.versionOrdinal = other.versionOrdinal;
    this.uuidLSBs = other.uuidLSBs;
    this.uuidMSBs = other.uuidMSBs;
  }


  @Override
  public int getMembershipPort() {
    return this.udpPort;
  }


  @Override
  public boolean isPreferredForCoordinator() {
    return this.preferredForCoordinator;
  }


  @Override
  public void setPreferredForCoordinator(boolean preferred) {
    this.preferredForCoordinator = preferred;
  }


  @Override
  public String getDurableId() {
    return durableId;
  }

  @Override
  public int getDurableTimeout() {
    return durableTimeout;
  }

  @Override
  public InetAddress getInetAddress() {
    return this.inetAddr;
  }

  @Override

  public short getVersionOrdinal() {
    return this.versionOrdinal;
  }

  @Override
  public void setVersionOrdinal(short versionOrdinal) {
    this.versionOrdinal = versionOrdinal;
  }

  @Override
  public void setUUID(UUID u) {
    if (u == null) {
      this.uuidLSBs = 0;
      this.uuidMSBs = 0;
    } else {
      this.uuidLSBs = u.getLeastSignificantBits();
      this.uuidMSBs = u.getMostSignificantBits();
    }
  }

  /**
   * return the jgroups logical address for this member, if it's been established
   */
  @Override
  public UUID getUUID() {
    if (this.uuidLSBs == 0 && this.uuidMSBs == 0) {
      return null;
    }
    return new UUID(this.uuidMSBs, this.uuidLSBs);
  }

  @Override
  public long getUuidMostSignificantBits() {
    return this.uuidMSBs;
  }

  @Override
  public long getUuidLeastSignificantBits() {
    return this.uuidLSBs;
  }

  /*
   * implements the java.lang.Comparable interface
   *
   * @see java.lang.Comparable
   *
   * @param o - the Object to be compared
   *
   * @return a negative integer, zero, or a positive integer as this object is less than, equal to,
   * or greater than the specified object.
   *
   * @exception java.lang.ClassCastException - if the specified object's type prevents it from being
   * compared to this Object.
   */

  @Override
  public int compareTo(GMSMemberData o) {
    return compareTo(o, true);
  }

  @Override
  public int compareTo(MemberData o, boolean compareUUIDs) {
    return compareTo(o, compareUUIDs, true);
  }

  @Override
  public int compareTo(MemberData o, boolean compareUUIDs, boolean compareViewIds) {
    if (o == this) {
      return 0;
    }
    // obligatory type check
    if (o == null) {
      throw new ClassCastException(
          "GMSMember.compareTo(): comparison between different classes");
    }
    byte[] myAddr = inetAddr.getAddress();
    GMSMemberData his = (GMSMemberData) o;
    byte[] hisAddr = his.inetAddr.getAddress();
    if (myAddr != hisAddr) {
      for (int idx = 0; idx < myAddr.length; idx++) {
        if (idx >= hisAddr.length) {
          return 1;
        } else if (myAddr[idx] > hisAddr[idx]) {
          return 1;
        } else if (myAddr[idx] < hisAddr[idx]) {
          return -1;
        }
      }
      // After checking both addresses we have only gone up to myAddr.length, their address could be
      // longer
      if (hisAddr.length > myAddr.length) {
        return -1;
      }
    }
    if (udpPort < his.udpPort)
      return -1;
    if (his.udpPort < udpPort)
      return 1;
    int result = 0;

    // bug #41983, address of kill-9'd member is reused
    // before it can be ejected from membership
    if (compareViewIds && this.vmViewId >= 0 && his.vmViewId >= 0) {
      if (this.vmViewId < his.vmViewId) {
        result = -1;
      } else if (his.vmViewId < this.vmViewId) {
        result = 1;
      }
    }
    if (compareUUIDs && result == 0 && this.uuidMSBs != 0 && his.uuidMSBs != 0) {
      if (this.uuidMSBs < his.uuidMSBs) {
        result = -1;
      } else if (his.uuidMSBs < this.uuidMSBs) {
        result = 1;
      } else if (this.uuidLSBs < his.uuidLSBs) {
        result = -1;
      } else if (his.uuidLSBs < this.uuidLSBs) {
        result = 1;
      }
    }
    return result;
  }


  @Override
  public int compareAdditionalData(MemberData o) {
    GMSMemberData his = (GMSMemberData) o;
    int result = 0;
    if (this.uuidMSBs != 0 && his.uuidMSBs != 0) {
      if (this.uuidMSBs < his.uuidMSBs) {
        result = -1;
      } else if (his.uuidMSBs < this.uuidMSBs) {
        result = 1;
      } else if (this.uuidLSBs < his.uuidLSBs) {
        result = -1;
      } else if (his.uuidLSBs < this.uuidLSBs) {
        result = 1;
      }
    }
    return result;
  }

  @Override
  public int getVmPid() {
    return processId;
  }

  @Override
  public boolean equals(Object obj) {
    // GemStone fix for 29125
    if ((obj == null) || !(obj instanceof GMSMemberData)) {
      return false;
    }
    return compareTo((GMSMemberData) obj) == 0;
  }

  @Override
  public int hashCode() {
    if (this.inetAddr == null) {
      return this.udpPort;
    }
    return this.udpPort + inetAddr.hashCode();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(100);

    sb.append("MemberData[");
    if (name != null && name.length() > 0) {
      sb.append("name=").append(name);
    }
    sb.append(";addr=").append(inetAddr).append(";port=").append(udpPort)
        .append(";kind=").append(vmKind).append(";processId=").append(processId)
        .append(";viewId=").append(vmViewId);
    if (versionOrdinal != Version.CURRENT_ORDINAL) {
      sb.append(";version=").append(versionOrdinal);
    }
    sb.append("]");
    return sb.toString();
  }


  @Override
  public boolean isNetworkPartitionDetectionEnabled() {
    return networkPartitionDetectionEnabled;
  }


  @Override
  public byte getMemberWeight() {
    return memberWeight;
  }

  @Override
  public InetAddress getInetAddr() {
    return inetAddr;
  }


  @Override
  public int getProcessId() {
    return processId;
  }


  @Override
  public byte getVmKind() {
    return vmKind;
  }


  @Override
  public int getVmViewId() {
    return vmViewId;
  }


  @Override
  public void setVmViewId(int id) {
    this.vmViewId = id;
  }


  @Override
  public int getDirectChannelPort() {
    return directPort;
  }


  @Override
  public String getName() {
    return name;
  }


  @Override
  public String[] getRoles() {
    return groups;
  }

  @Override
  public void setUdpPort(int udpPort) {
    this.udpPort = udpPort;
  }


  @Override
  public void setNetworkPartitionDetectionEnabled(boolean networkPartitionDetectionEnabled) {
    this.networkPartitionDetectionEnabled = networkPartitionDetectionEnabled;
  }

  @Override
  public void setMemberWeight(byte memberWeight) {
    this.memberWeight = memberWeight;
  }

  @Override
  public void setInetAddr(InetAddress inetAddr) {
    this.inetAddr = inetAddr;
  }


  @Override
  public void setProcessId(int processId) {
    this.processId = processId;
  }


  @Override
  public void setVmKind(int vmKind) {
    this.vmKind = (byte) vmKind;
  }


  @Override
  public void setVersion(Version v) {
    this.versionOrdinal = v.ordinal();
  }

  @Override
  public void setDirectChannelPort(int directPort) {
    this.directPort = directPort;
  }


  @Override
  public void setName(String name) {
    this.name = name;
  }


  @Override
  public String[] getGroups() {
    return groups;
  }

  @Override
  public void setGroups(String[] groups) {
    this.groups = groups;
  }


  @Override
  public void setPort(int p) {
    this.udpPort = p;
  }

  /**
   * checks to see if this address has UUID information needed to send messages via JGroups
   */
  @Override
  public boolean hasUUID() {
    return !(this.uuidLSBs == 0 && this.uuidMSBs == 0);
  }

  static final int NPD_ENABLED_BIT = 0x01;
  static final int PREFERRED_FOR_COORD_BIT = 0x02;
  static final int VERSION_BIT = 0x8;

  @Override
  public void writeEssentialData(DataOutput out,
      SerializationContext context) throws IOException {
    Version.writeOrdinal(out, this.versionOrdinal, true);

    int flags = 0;
    if (networkPartitionDetectionEnabled)
      flags |= NPD_ENABLED_BIT;
    if (preferredForCoordinator)
      flags |= PREFERRED_FOR_COORD_BIT;
    out.writeShort(flags);

    StaticSerialization.writeInetAddress(inetAddr, out);
    out.writeInt(udpPort);
    out.writeInt(vmViewId);
    out.writeLong(uuidMSBs);
    out.writeLong(uuidLSBs);
    if (context.getSerializationVersion().ordinal() >= Version.GEODE_1_2_0.ordinal()) {
      out.writeByte(vmKind);
    }
  }

  @Override
  public String getHostName() {
    return hostName;
  }

  @Override
  public void setHostName(String hostName) {
    this.hostName = hostName;
  }

  @Override
  public void readEssentialData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    this.versionOrdinal = Version.readOrdinal(in);

    int flags = in.readShort();
    this.networkPartitionDetectionEnabled = (flags & NPD_ENABLED_BIT) != 0;
    this.preferredForCoordinator = (flags & PREFERRED_FOR_COORD_BIT) != 0;

    this.inetAddr = StaticSerialization.readInetAddress(in);
    if (this.inetAddr != null) {
      this.hostName = inetAddr.getHostName();
    }
    this.udpPort = in.readInt();
    this.vmViewId = in.readInt();
    this.uuidMSBs = in.readLong();
    this.uuidLSBs = in.readLong();
    if (context.getSerializationVersion().ordinal() >= Version.GEODE_1_2_0.ordinal()) {
      this.vmKind = in.readByte();
    }
    this.isPartial = true;
  }


  @Override
  public boolean hasAdditionalData() {
    return uuidMSBs != 0 || uuidLSBs != 0 || memberWeight != 0;
  }


  @Override
  public void writeAdditionalData(DataOutput out) throws IOException {
    out.writeLong(uuidMSBs);
    out.writeLong(uuidLSBs);
    out.write(memberWeight);
  }


  @Override
  public void readAdditionalData(DataInput in) throws ClassNotFoundException, IOException {
    try {
      this.uuidMSBs = in.readLong();
      this.uuidLSBs = in.readLong();
      memberWeight = (byte) (in.readByte() & 0xFF);
    } catch (EOFException e) {
      // some IDs do not have UUID or membership weight information
    }
  }

  @Override
  public void setDurableTimeout(int newValue) {
    durableTimeout = newValue;
  }

  @Override
  public void setDurableId(String id) {
    durableId = id;
  }
}
