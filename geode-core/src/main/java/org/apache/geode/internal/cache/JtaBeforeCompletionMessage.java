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

import java.util.Collections;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.Cache;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ReliableReplyProcessor21;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.logging.internal.LogService;

public class JtaBeforeCompletionMessage extends TXMessage {
  private static final Logger logger = LogService.getLogger();

  public JtaBeforeCompletionMessage() {
    // do nothing
  }

  public JtaBeforeCompletionMessage(int txUniqId, InternalDistributedMember onBehalfOfClientMember,
      ReplyProcessor21 processor) {
    super(txUniqId, onBehalfOfClientMember, processor);
  }

  public static ReliableReplyProcessor21 send(Cache cache, int txUniqId,
      InternalDistributedMember onBehalfOfClientMember, DistributedMember recipient) {
    final InternalDistributedSystem system =
        (InternalDistributedSystem) cache.getDistributedSystem();
    final Set recipients = Collections.singleton(recipient);
    ReliableReplyProcessor21 response = new ReliableReplyProcessor21(system, recipients);
    JtaBeforeCompletionMessage msg =
        new JtaBeforeCompletionMessage(txUniqId, onBehalfOfClientMember, response);
    msg.setRecipients(recipients);
    system.getDistributionManager().putOutgoing(msg);
    return response;

  }

  @Override
  protected boolean operateOnTx(TXId txId, ClusterDistributionManager dm) {
    InternalCache cache = dm.getCache();
    TXManagerImpl txMgr = cache.getTXMgr();
    if (logger.isDebugEnabled()) {
      logger.debug("JTA: Calling beforeCompletion for :{}", txId);
    }
    // Check if jta has been completed, possible due to tx failover.
    // No need to execute beforeCompletion if already completed.
    if (txMgr.isHostedTxRecentlyCompleted(txId)) {
      return true;
    }

    txMgr.getTXState().beforeCompletion();
    return true;
  }

  @Override
  public int getDSFID() {
    return JTA_BEFORE_COMPLETION_MESSAGE;
  }
}
