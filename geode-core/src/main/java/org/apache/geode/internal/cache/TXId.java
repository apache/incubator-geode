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
package org.apache.geode.internal.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.cache.TransactionId;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.DSFIDFactory;
import org.apache.geode.internal.ExternalizableDSFID;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.serialization.Version;

/**
 * The implementation of the {@link TransactionId} interface stored in the transaction state and
 * used, among other things, to uniquely identify a transaction in a confederation of transaction
 * participants (currently VM in a Distributed System).
 *
 * @since GemFire 4.0
 * @see TXManagerImpl#begin
 * @see org.apache.geode.cache.CacheTransactionManager#getTransactionId
 */
public class TXId extends ExternalizableDSFID implements TransactionId {

  /** The domain of a transaction, currently the VM's unique identifier */
  private InternalDistributedMember memberId;
  /** Per unique identifier within the transactions memberId */
  private int uniqId;

  /**
   * Default constructor meant for the Externalizable
   */
  public TXId() {}

  /**
   * Constructor for the Transation Manager, the birth place of TXId objects. The object is
   * Serializable mainly because of the identifier type provided by JGroups.
   */
  public TXId(InternalDistributedMember memberId, int uniqId) {
    this.memberId = memberId;
    this.uniqId = uniqId;
  }

  @Override
  public InternalDistributedMember getMemberId() {
    return this.memberId;
  }

  @Override
  public String toString() {
    return ("TXId: " + this.memberId + ':' + this.uniqId);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof TXId)) {
      return false;
    }

    TXId otx = (TXId) o;
    return (otx.uniqId == this.uniqId && ((otx.memberId == null && this.memberId == null)
        || (otx.memberId != null && this.memberId != null && otx.memberId.equals(this.memberId))));

  }

  @Override
  public int hashCode() {
    int retval = this.uniqId;
    if (this.memberId != null)
      retval = retval * 37 + this.memberId.hashCode();
    return retval;
  }

  @Override
  public int getDSFID() {
    return TRANSACTION_ID;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    out.writeInt(this.uniqId);
    InternalDataSerializer.invokeToData(this.memberId, out);
  }

  @Override
  public void fromData(DataInput in,
      SerializationContext context) throws IOException, ClassNotFoundException {
    this.uniqId = in.readInt();
    this.memberId = DSFIDFactory.readInternalDistributedMember(in);
  }

  public static TXId createFromData(DataInput in) throws IOException, ClassNotFoundException {
    TXId result = new TXId();
    InternalDataSerializer.invokeFromData(result, in);
    return result;
  }

  public int getUniqId() {
    return this.uniqId;
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }

}
