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
package org.apache.geode.distributed.internal.membership.api;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import org.apache.commons.validator.routines.InetAddressValidator;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.serialization.StaticSerialization;
import org.apache.geode.internal.serialization.UnsupportedSerializationVersionException;
import org.apache.geode.internal.serialization.Version;

/**
 * An implementation of {@link MemberIdentifier}
 */
public class MemberIdentifierImpl implements MemberIdentifier, DataSerializableFixedID {
  /** serialization bit flag */
  private static final int NPD_ENABLED_BIT = 0x1;
  /** serialization bit flag */
  private static final int COORD_ENABLED_BIT = 0x2;
  /** partial ID bit flag */
  private static final int PARTIAL_ID_BIT = 0x4;
  /** product version bit flag */
  private static final int VERSION_BIT = 0x8;
  /** The versions in which this message was modified */
  @Immutable
  private static final Version[] dsfidVersions = new Version[] {
      Version.GFE_71, Version.GFE_90};
  private MemberData memberData; // the underlying member object
  /**
   * whether this is a partial member ID (without roles, durable attributes). We use partial IDs in
   * EventID objects to reduce their size. It would be better to use canonical IDs but there is
   * currently no central mechanism that would allow that for both server and client identifiers
   */
  private boolean isPartial;
  /**
   * Unique tag (such as randomly generated bytes) to help enforce uniqueness. Note: this should be
   * displayable.
   */
  private String uniqueTag = null;
  private transient Version versionObj = Version.CURRENT;

  public MemberIdentifierImpl() {}

  public MemberIdentifierImpl(
      MemberData memberData, String uniqueTag) {
    this.memberData = memberData;
    this.isPartial = memberData.isPartial();
    this.uniqueTag = uniqueTag;

    short version = memberData.getVersionOrdinal();
    try {
      this.versionObj = Version.fromOrdinal(version);
    } catch (UnsupportedSerializationVersionException e) {
      this.versionObj = Version.CURRENT;
    }
  }

  public int getVmPid() {
    return memberData.getProcessId();
  }

  public void setDurableTimeout(int newValue) {
    memberData.setDurableTimeout(newValue);
  }

  public void setDurableId(String id) {
    memberData.setDurableId(id);
  }

  /**
   * Replace the current member data with the given member data. This can be used to fill out an
   * InternalDistributedMember that was created from a partial data created by
   * readEssentialData.
   *
   * @param m the replacement member data
   */
  public void setMemberData(MemberData m) {
    this.memberData = m;
  }

  /**
   * Return the underlying host address
   *
   * @return the underlying host address
   */
  public InetAddress getInetAddress() {
    return memberData.getInetAddress();
  }

  /**
   * Return the underlying port (membership port)
   *
   * @return the underlying membership port
   */
  public int getMembershipPort() {
    return memberData.getMembershipPort();
  }

  @Override
  public short getVersionOrdinal() {
    return versionObj == null ? memberData.getVersionOrdinal() : versionObj.ordinal();
  }

  /**
   * Returns the port on which the direct channel runs
   */
  public int getDirectChannelPort() {
    assert !this.isPartial;
    return memberData.getDirectChannelPort();
  }

  /**
   * [GemStone] Returns the kind of VM that hosts the distribution manager with this address.
   *
   * @see MemberIdentifier#NORMAL_DM_TYPE
   */
  public int getVmKind() {
    return memberData.getVmKind();
  }

  @Override
  public int getMemberWeight() {
    return memberData.getMemberWeight();
  }

  /**
   * Returns the membership view ID that this member was born in. For backward compatibility reasons
   * this is limited to 16 bits.
   */
  public int getVmViewId() {
    return memberData.getVmViewId();
  }

  @Override
  public boolean preferredForCoordinator() {
    return memberData.isPreferredForCoordinator();
  }

  @Override
  public List<String> getGroups() {
    return Collections.unmodifiableList(Arrays.asList(memberData.getGroups()));
  }

  @Override
  public void setVmViewId(int p) {
    memberData.setVmViewId(p);
    cachedToString = null;
  }

  @Override
  public void setPreferredForCoordinator(boolean preferred) {
    memberData.setPreferredForCoordinator(preferred);
    cachedToString = null;
  }

  @Override
  public void setDirectChannelPort(int dcPort) {
    memberData.setDirectChannelPort(dcPort);
    cachedToString = null;
  }

  @Override
  public void setVmKind(int dmType) {
    memberData.setVmKind(dmType);
    cachedToString = null;
  }

  protected void setGroups(String[] newGroups) {
    this.memberData.setGroups(newGroups);
    cachedToString = null;
  }

  /**
   * Returns the name of this member's distributed system connection or null if no name was
   * specified.
   */
  public String getName() {
    String result = memberData.getName();
    if (result == null) {
      result = "";
    }
    return result;
  }

  public int compare(MemberIdentifierImpl other) {
    return this.compareTo(other, false, true);
  }

  protected int compareTo(MemberIdentifierImpl other, boolean compareMemberData,
      boolean compareViewIds) {
    int myPort = getMembershipPort();
    int otherPort = other.getMembershipPort();
    if (myPort < otherPort)
      return -1;
    if (myPort > otherPort)
      return 1;

    InetAddress myAddr = getInetAddress();
    InetAddress otherAddr = other.getInetAddress();

    // Discard null cases
    if (myAddr == null && otherAddr == null) {
      return 0;
    } else if (myAddr == null) {
      return -1;
    } else if (otherAddr == null)
      return 1;

    byte[] myBytes = myAddr.getAddress();
    byte[] otherBytes = otherAddr.getAddress();

    if (myBytes != otherBytes) {
      for (int i = 0; i < myBytes.length; i++) {
        if (i >= otherBytes.length)
          return -1; // same as far as they go, but shorter...
        if (myBytes[i] < otherBytes[i])
          return -1;
        if (myBytes[i] > otherBytes[i])
          return 1;
      }
      if (myBytes.length > otherBytes.length)
        return 1; // same as far as they go, but longer...
    }

    String myName = getName();
    String otherName = other.getName();
    if (!(other.isPartial || this.isPartial)) {
      if (myName == null && otherName == null) {
        // do nothing
      } else if (myName == null) {
        return -1;
      } else if (otherName == null) {
        return 1;
      } else {
        int i = myName.compareTo(otherName);
        if (i != 0) {
          return i;
        }
      }
    }

    if (this.uniqueTag == null && other.uniqueTag == null) {
      if (compareViewIds) {
        // not loners, so look at P2P view ID
        int thisViewId = getVmViewId();
        int otherViewId = other.getVmViewId();
        if (thisViewId >= 0 && otherViewId >= 0) {
          if (thisViewId < otherViewId) {
            return -1;
          } else if (thisViewId > otherViewId) {
            return 1;
          } // else they're the same, so continue
        }
      }
    } else if (this.uniqueTag == null) {
      return -1;
    } else if (other.uniqueTag == null) {
      return 1;
    } else {
      int i = this.uniqueTag.compareTo(other.uniqueTag);
      if (i != 0) {
        return i;
      }
    }

    if (compareMemberData && this.memberData != null && other.memberData != null) {
      return this.memberData.compareAdditionalData(other.memberData);
    } else {
      return 0;
    }
  }

  /**
   * An InternalDistributedMember created for a test or via readEssentialData will be a Partial ID,
   * possibly not having ancillary info like "name".
   *
   * @return true if this is a partial ID
   */
  public boolean isPartial() {
    return isPartial;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    // GemStone fix for 29125
    if (!(obj instanceof MemberIdentifierImpl)) {
      return false;
    }
    MemberIdentifierImpl other = (MemberIdentifierImpl) obj;

    int myPort = getMembershipPort();
    int otherPort = other.getMembershipPort();
    if (myPort != otherPort) {
      return false;
    }

    InetAddress myAddr = getInetAddress();
    InetAddress otherAddr = other.getInetAddress();
    if (myAddr == null && otherAddr == null) {
      return true;
    } else if (!Objects.equals(myAddr, otherAddr)) {
      return false;
    }

    if (!isPartial() && !other.isPartial()) {
      if (!Objects.equals(getName(), other.getName())) {
        return false;
      }
    }

    if (this.uniqueTag == null && other.uniqueTag == null) {
      // not loners, so look at P2P view ID
      int thisViewId = getVmViewId();
      int otherViewId = other.getVmViewId();
      if (thisViewId >= 0 && otherViewId >= 0) {
        if (thisViewId != otherViewId) {
          return false;
        } // else they're the same, so continue
      }
    } else if (!Objects.equals(this.uniqueTag, other.uniqueTag)) {
      return false;
    }

    if (this.memberData != null && other.memberData != null) {
      if (0 != this.memberData.compareAdditionalData(other.memberData)) {
        return false;
      }
    }

    // purposely avoid checking roles
    // @todo Add durableClientAttributes to equals

    return true;
  }

  @Override
  public int hashCode() {
    int result = 0;
    result = result + memberData.getInetAddress().hashCode();
    result = result + getMembershipPort();
    return result;
  }

  private String shortName(String hostname) {
    if (hostname == null)
      return "<null inet_addr hostname>";
    int index = hostname.indexOf('.');

    if (index > 0 && !Character.isDigit(hostname.charAt(0)))
      return hostname.substring(0, index);
    else
      return hostname;
  }


  /** the cached string description of this object */
  private transient String cachedToString;

  @Override
  public String toString() {
    String result = cachedToString;
    if (result == null) {
      final StringBuilder sb = new StringBuilder();
      addFixedToString(sb, false);

      // add version if not current
      short version = memberData.getVersionOrdinal();
      if (version != Version.CURRENT.ordinal()) {
        sb.append("(version:").append(Version.toString(version)).append(')');
      }

      // leave out Roles on purpose

      result = sb.toString();
      cachedToString = result;
    }
    return result;
  }

  public void addFixedToString(StringBuilder sb, boolean useIpAddress) {
    // Note: This method is used to generate the HARegion name. If it is changed, memory and GII
    // issues will occur in the case of clients with subscriptions during rolling upgrade.
    String host;

    InetAddress add = getInetAddress();
    if (add.isMulticastAddress() || useIpAddress)
      host = add.getHostAddress();
    else {
      String hostName = memberData.getHostName();
      InetAddressValidator inetAddressValidator = InetAddressValidator.getInstance();
      boolean isIpAddress = inetAddressValidator.isValid(hostName);
      host = isIpAddress ? hostName : shortName(hostName);
    }

    sb.append(host);

    String myName = getName();
    int vmPid = memberData.getProcessId();
    int vmKind = memberData.getVmKind();
    if (vmPid > 0 || vmKind != MemberIdentifier.NORMAL_DM_TYPE || !"".equals(myName)) {
      sb.append("(");

      if (!"".equals(myName)) {
        sb.append(myName);
        if (vmPid > 0) {
          sb.append(':');
        }
      }

      if (vmPid > 0)
        sb.append(vmPid);

      String vmStr = "";
      switch (vmKind) {
        case MemberIdentifier.NORMAL_DM_TYPE:
          // vmStr = ":local"; // let this be silent
          break;
        case MemberIdentifier.LOCATOR_DM_TYPE:
          vmStr = ":locator";
          break;
        case MemberIdentifier.ADMIN_ONLY_DM_TYPE:
          vmStr = ":admin";
          break;
        case MemberIdentifier.LONER_DM_TYPE:
          vmStr = ":loner";
          break;
        default:
          vmStr = ":<unknown:" + vmKind + ">";
          break;
      }
      sb.append(vmStr);
      sb.append(")");
    }
    if (vmKind != MemberIdentifier.LONER_DM_TYPE
        && memberData.isPreferredForCoordinator()) {
      sb.append("<ec>");
    }
    int vmViewId = getVmViewId();
    if (vmViewId >= 0) {
      sb.append("<v" + vmViewId + ">");
    }
    sb.append(":");
    sb.append(getMembershipPort());

    if (vmKind == MemberIdentifier.LONER_DM_TYPE) {
      // add some more info that was added in 4.2.1 for loner bridge clients
      // impact on non-bridge loners is ok
      if (this.uniqueTag != null && this.uniqueTag.length() != 0) {
        sb.append(":").append(this.uniqueTag);
      }
      String name = getName();
      if (name.length() != 0) {
        sb.append(":").append(name);
      }
    }
  }

  private short readVersion(int flags, DataInput in) throws IOException {
    if ((flags & VERSION_BIT) != 0) {
      short version = Version.readOrdinal(in);
      this.versionObj = Version.fromOrdinalNoThrow(version, false);
      return version;
    } else {
      // prior to 7.1 member IDs did not serialize their version information
      Version v = StaticSerialization.getVersionForDataStreamOrNull(in);
      if (v != null) {
        this.versionObj = v;
        return v.ordinal();
      }
      return Version.CURRENT_ORDINAL;
    }
  }

  /**
   * For Externalizable
   *
   * @see Externalizable
   */
  public void writeExternal(ObjectOutput out) throws IOException {
    assert memberData.getVmKind() > 0;

    // do it the way we like
    byte[] address = getInetAddress().getAddress();

    out.writeInt(address.length); // IPv6 compatible
    out.write(address);
    out.writeInt(getMembershipPort());

    StaticSerialization.writeString(memberData.getHostName(), out);

    int flags = 0;
    if (memberData.isNetworkPartitionDetectionEnabled())
      flags |= NPD_ENABLED_BIT;
    if (memberData.isPreferredForCoordinator())
      flags |= COORD_ENABLED_BIT;
    if (this.isPartial)
      flags |= PARTIAL_ID_BIT;
    // always write product version but enable reading from older versions
    // that do not have it
    flags |= VERSION_BIT;
    out.writeByte((byte) (flags & 0xff));

    out.writeInt(memberData.getDirectChannelPort());
    out.writeInt(memberData.getProcessId());
    out.writeInt(memberData.getVmKind());
    out.writeInt(memberData.getVmViewId());
    StaticSerialization.writeStringArray(memberData.getGroups(), out);

    StaticSerialization.writeString(memberData.getName(), out);
    StaticSerialization.writeString(this.uniqueTag, out);
    String durableId = memberData.getDurableId();
    StaticSerialization.writeString(durableId == null ? "" : durableId, out);
    StaticSerialization.writeInteger(
        Integer.valueOf(durableId == null ? 300 : memberData.getDurableTimeout()),
        out);
    Version.writeOrdinal(out, memberData.getVersionOrdinal(), true);
    memberData.writeAdditionalData(out);
  }

  /**
   * For Externalizable
   *
   * @see Externalizable
   */
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    int len = in.readInt(); // IPv6 compatible
    byte addr[] = new byte[len];
    in.readFully(addr);
    InetAddress inetAddr = InetAddress.getByAddress(addr);
    int port = in.readInt();

    String hostName = StaticSerialization.readString(in);

    int flags = in.readUnsignedByte();
    boolean sbEnabled = (flags & NPD_ENABLED_BIT) != 0;
    boolean elCoord = (flags & COORD_ENABLED_BIT) != 0;
    this.isPartial = (flags & PARTIAL_ID_BIT) != 0;

    int dcPort = in.readInt();
    int vmPid = in.readInt();
    int vmKind = in.readInt();
    int vmViewId = in.readInt();
    String[] groups = StaticSerialization.readStringArray(in);

    String name = StaticSerialization.readString(in);
    this.uniqueTag = StaticSerialization.readString(in);
    String durableId = StaticSerialization.readString(in);
    int durableTimeout = in.readInt();

    short version = readVersion(flags, in);

    memberData = MemberDataBuilder.newBuilder(inetAddr, hostName)
        .setMembershipPort(port)
        .setDirectChannelPort(dcPort)
        .setName(name)
        .setNetworkPartitionDetectionEnabled(sbEnabled)
        .setPreferredForCoordinator(elCoord)
        .setVersionOrdinal(version)
        .setVmPid(vmPid)
        .setVmKind(vmKind)
        .setVmViewId(vmViewId)
        .setGroups(groups)
        .setDurableId(durableId)
        .setDurableTimeout(durableTimeout)
        .build();
    if (version >= Version.GFE_90.ordinal()) {
      try {
        memberData.readAdditionalData(in);
      } catch (java.io.EOFException e) {
        // old version
      }
    }
    assert memberData.getVmKind() > 0;
  }

  @Override
  public int getDSFID() {
    return MEMBER_IDENTIFIER;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    toDataPre_GFE_9_0_0_0(out, context);
    if (memberData.getVersionOrdinal() >= Version.GFE_90.ordinal()) {
      getMemberData().writeAdditionalData(out);
    }
  }

  public void toDataPre_GFE_9_0_0_0(DataOutput out, SerializationContext context)
      throws IOException {
    // Assert.assertTrue(vmKind > 0);
    // NOTE: If you change the serialized format of this class
    // then bump Connection.HANDSHAKE_VERSION since an
    // instance of this class is sent during Connection handshake.
    StaticSerialization.writeInetAddress(getInetAddress(), out);
    out.writeInt(getMembershipPort());

    StaticSerialization.writeString(memberData.getHostName(), out);

    int flags = 0;
    if (memberData.isNetworkPartitionDetectionEnabled())
      flags |= NPD_ENABLED_BIT;
    if (memberData.isPreferredForCoordinator())
      flags |= COORD_ENABLED_BIT;
    if (this.isPartial)
      flags |= PARTIAL_ID_BIT;
    // always write product version but enable reading from older versions
    // that do not have it
    flags |= VERSION_BIT;

    out.writeByte((byte) (flags & 0xff));

    out.writeInt(memberData.getDirectChannelPort());
    out.writeInt(memberData.getProcessId());
    int vmKind = memberData.getVmKind();
    out.writeByte(vmKind);
    StaticSerialization.writeStringArray(memberData.getGroups(), out);

    StaticSerialization.writeString(memberData.getName(), out);
    if (vmKind == MemberIdentifier.LONER_DM_TYPE) {
      StaticSerialization.writeString(this.uniqueTag, out);
    } else { // added in 6.5 for unique identifiers in P2P
      StaticSerialization.writeString(String.valueOf(memberData.getVmViewId()), out);
    }
    String durableId = memberData.getDurableId();
    StaticSerialization.writeString(durableId == null ? "" : durableId, out);
    StaticSerialization.writeInteger(
        Integer.valueOf(durableId == null ? 300 : memberData.getDurableTimeout()),
        out);

    short version = memberData.getVersionOrdinal();
    Version.writeOrdinal(out, version, true);
  }

  public void toDataPre_GFE_7_1_0_0(DataOutput out, SerializationContext context)
      throws IOException {
    assert memberData.getVmKind() > 0;
    // disabled to allow post-connect setting of the port for loner systems
    // Assert.assertTrue(getPort() > 0);
    // if (this.getPort() == 0) {
    // InternalDistributedSystem.getLogger().warning(String.format("%s",
    // "Serializing ID with zero port", new Exception("Stack trace")));
    // }

    // NOTE: If you change the serialized format of this class
    // then bump Connection.HANDSHAKE_VERSION since an
    // instance of this class is sent during Connection handshake.
    StaticSerialization.writeInetAddress(getInetAddress(), out);
    out.writeInt(getMembershipPort());

    StaticSerialization.writeString(memberData.getHostName(), out);

    int flags = 0;
    if (memberData.isNetworkPartitionDetectionEnabled())
      flags |= NPD_ENABLED_BIT;
    if (memberData.isPreferredForCoordinator())
      flags |= COORD_ENABLED_BIT;
    if (this.isPartial)
      flags |= PARTIAL_ID_BIT;
    out.writeByte((byte) (flags & 0xff));

    out.writeInt(memberData.getDirectChannelPort());
    out.writeInt(memberData.getProcessId());
    out.writeByte(memberData.getVmKind());
    StaticSerialization.writeStringArray(memberData.getGroups(), out);

    StaticSerialization.writeString(memberData.getName(), out);
    int vmKind = memberData.getVmKind();
    if (vmKind == MemberIdentifier.LONER_DM_TYPE) {
      StaticSerialization.writeString(this.uniqueTag, out);
    } else { // added in 6.5 for unique identifiers in P2P
      StaticSerialization.writeString(String.valueOf(memberData.getVmViewId()), out);
    }
    String durableId = memberData.getDurableId();
    StaticSerialization.writeString(durableId == null ? "" : durableId, out);
    StaticSerialization.writeInteger(
        Integer.valueOf(durableId == null ? 300 : memberData.getDurableTimeout()),
        out);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    fromDataPre_GFE_9_0_0_0(in, context);
    // just in case this is just a non-versioned read
    // from a file we ought to check the version
    if (getMemberData().getVersionOrdinal() >= Version.GFE_90.ordinal()) {
      try {
        memberData.readAdditionalData(in);
      } catch (EOFException e) {
        // nope - it's from a pre-GEODE client or WAN site
      }
    }
  }

  public void fromDataPre_GFE_9_0_0_0(DataInput in, DeserializationContext context)
      throws IOException, ClassNotFoundException {
    InetAddress inetAddr = StaticSerialization.readInetAddress(in);
    int port = in.readInt();

    String hostName = StaticSerialization.readString(in);

    int flags = in.readUnsignedByte();
    boolean sbEnabled = (flags & NPD_ENABLED_BIT) != 0;
    boolean elCoord = (flags & COORD_ENABLED_BIT) != 0;
    this.isPartial = (flags & PARTIAL_ID_BIT) != 0;

    int dcPort = in.readInt();
    int vmPid = in.readInt();
    int vmKind = in.readUnsignedByte();
    String[] groups = StaticSerialization.readStringArray(in);
    int vmViewId = -1;

    String name = StaticSerialization.readString(in);
    if (vmKind == MemberIdentifier.LONER_DM_TYPE) {
      this.uniqueTag = StaticSerialization.readString(in);
    } else {
      String str = StaticSerialization.readString(in);
      if (str != null) { // backward compatibility from earlier than 6.5
        vmViewId = Integer.parseInt(str);
      }
    }

    String durableId = StaticSerialization.readString(in);
    int durableTimeout = in.readInt();

    short version = readVersion(flags, in);

    memberData = MemberDataBuilder.newBuilder(inetAddr, hostName)
        .setMembershipPort(port)
        .setDirectChannelPort(dcPort)
        .setName(name)
        .setNetworkPartitionDetectionEnabled(sbEnabled)
        .setPreferredForCoordinator(elCoord)
        .setVersionOrdinal(version)
        .setVmPid(vmPid)
        .setVmKind(vmKind)
        .setVmViewId(vmViewId)
        .setGroups(groups)
        .setDurableId(durableId)
        .setDurableTimeout(durableTimeout)
        .build();

    assert memberData.getVmKind() > 0;
    // Assert.assertTrue(getPort() > 0);
  }

  public void fromDataPre_GFE_7_1_0_0(DataInput in, DeserializationContext context)
      throws IOException, ClassNotFoundException {
    InetAddress inetAddr = StaticSerialization.readInetAddress(in);
    int port = in.readInt();

    String hostName = StaticSerialization.readString(in);

    int flags = in.readUnsignedByte();
    boolean sbEnabled = (flags & NPD_ENABLED_BIT) != 0;
    boolean elCoord = (flags & COORD_ENABLED_BIT) != 0;
    this.isPartial = (flags & PARTIAL_ID_BIT) != 0;

    int dcPort = in.readInt();
    int vmPid = in.readInt();
    int vmKind = in.readUnsignedByte();
    String[] groups = StaticSerialization.readStringArray(in);
    int vmViewId = -1;

    String name = StaticSerialization.readString(in);
    if (vmKind == MemberIdentifier.LONER_DM_TYPE) {
      this.uniqueTag = StaticSerialization.readString(in);
    } else {
      String str = StaticSerialization.readString(in);
      if (str != null) { // backward compatibility from earlier than 6.5
        vmViewId = Integer.parseInt(str);
      }
    }

    String durableId = StaticSerialization.readString(in);
    int durableTimeout = in.readInt();

    short version = readVersion(flags, in);

    memberData = MemberDataBuilder.newBuilder(inetAddr, hostName)
        .setMembershipPort(port)
        .setDirectChannelPort(dcPort)
        .setName(name)
        .setNetworkPartitionDetectionEnabled(sbEnabled)
        .setPreferredForCoordinator(elCoord)
        .setVersionOrdinal(version)
        .setVmPid(vmPid)
        .setVmKind(vmKind)
        .setVmViewId(vmViewId)
        .setGroups(groups)
        .setDurableId(durableId)
        .setDurableTimeout(durableTimeout)
        .build();

    assert memberData.getVmKind() > 0;
  }


  public void _readEssentialData(DataInput in, Function<InetAddress, String> hostnameResolver)
      throws IOException, ClassNotFoundException {
    this.isPartial = true;
    InetAddress inetAddr = StaticSerialization.readInetAddress(in);
    int port = in.readInt();

    String hostName = hostnameResolver.apply(inetAddr);

    int flags = in.readUnsignedByte();
    boolean sbEnabled = (flags & NPD_ENABLED_BIT) != 0;
    boolean elCoord = (flags & COORD_ENABLED_BIT) != 0;

    int vmKind = in.readUnsignedByte();
    int vmViewId = -1;

    if (vmKind == MemberIdentifier.LONER_DM_TYPE) {
      this.uniqueTag = StaticSerialization.readString(in);
    } else {
      String str = StaticSerialization.readString(in);
      if (str != null) { // backward compatibility from earlier than 6.5
        vmViewId = Integer.parseInt(str);
      }
    }

    String name = StaticSerialization.readString(in);

    memberData = MemberDataBuilder.newBuilder(inetAddr, hostName)
        .setMembershipPort(port)
        .setName(name)
        .setNetworkPartitionDetectionEnabled(sbEnabled)
        .setPreferredForCoordinator(elCoord)
        .setVersionOrdinal(StaticSerialization.getVersionForDataStream(in).ordinal())
        .setVmKind(vmKind)
        .setVmViewId(vmViewId)
        .build();

    if (StaticSerialization.getVersionForDataStream(in).compareTo(Version.GFE_90) == 0) {
      memberData.readAdditionalData(in);
    }
  }

  public void writeEssentialData(DataOutput out) throws IOException {
    assert memberData.getVmKind() > 0;
    StaticSerialization.writeInetAddress(getInetAddress(), out);
    out.writeInt(getMembershipPort());

    int flags = 0;
    if (memberData.isNetworkPartitionDetectionEnabled())
      flags |= NPD_ENABLED_BIT;
    if (memberData.isPreferredForCoordinator())
      flags |= COORD_ENABLED_BIT;
    flags |= PARTIAL_ID_BIT;
    out.writeByte((byte) (flags & 0xff));

    // out.writeInt(dcPort);
    byte vmKind = memberData.getVmKind();
    out.writeByte(vmKind);

    if (vmKind == MemberIdentifier.LONER_DM_TYPE) {
      StaticSerialization.writeString(this.uniqueTag, out);
    } else { // added in 6.5 for unique identifiers in P2P
      StaticSerialization.writeString(String.valueOf(memberData.getVmViewId()), out);
    }
    // write name last to fix bug 45160
    StaticSerialization.writeString(memberData.getName(), out);

    Version outputVersion = StaticSerialization.getVersionForDataStream(out);
    if (0 <= outputVersion.compareTo(Version.GFE_90)
        && outputVersion.compareTo(Version.GEODE_1_1_0) < 0) {
      memberData.writeAdditionalData(out);
    }
  }



  /**
   * Set the membership port. This is done in loner systems using client/server connection
   * information to help form a unique ID
   */
  public void setPort(int p) {
    assert memberData.getVmKind() == MemberIdentifier.LONER_DM_TYPE;
    this.memberData.setPort(p);
    cachedToString = null;
  }

  @Override
  public MemberData getMemberData() {
    return memberData;
  }

  @Override
  public String getHostName() {
    return memberData.getHostName();
  }

  public String getHost() {
    return this.memberData.getInetAddress().getCanonicalHostName();
  }

  public int getProcessId() {
    return memberData.getProcessId();
  }

  public String getId() {
    return toString();
  }

  public String getUniqueId() {
    StringBuilder sb = new StringBuilder();
    addFixedToString(sb, false);

    // add version if not current
    short version = memberData.getVersionOrdinal();
    if (version != Version.CURRENT.ordinal()) {
      sb.append("(version:").append(Version.toString(version)).append(')');
    }

    return sb.toString();
  }

  public void setVersionObjectForTest(Version v) {
    this.versionObj = v;
    memberData.setVersion(v);
  }

  public Version getVersionObject() {
    return this.versionObj;
  }

  @Override
  public Version[] getSerializationVersions() {
    return dsfidVersions;
  }

  @VisibleForTesting
  public void setUniqueTag(String tag) {
    uniqueTag = tag;
  }

  @Override
  public void setIsPartial(boolean value) {
    isPartial = value;
  }

}
