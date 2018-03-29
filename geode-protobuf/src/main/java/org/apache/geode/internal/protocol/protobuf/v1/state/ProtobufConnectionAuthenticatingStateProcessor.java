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
package org.apache.geode.internal.protocol.protobuf.v1.state;

import java.util.Properties;

import org.apache.shiro.subject.Subject;

import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.v1.MessageExecutionContext;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufOperationContext;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufSerializationService;
import org.apache.geode.internal.protocol.protobuf.v1.authentication.ShiroAuthorizer;
import org.apache.geode.internal.protocol.protobuf.v1.operations.security.HandshakeRequestOperationHandler;
import org.apache.geode.internal.protocol.protobuf.v1.state.exception.ConnectionStateException;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.AuthenticationFailedException;

public class ProtobufConnectionAuthenticatingStateProcessor
    implements ProtobufConnectionStateProcessor {
  private final SecurityService securityService;

  public ProtobufConnectionAuthenticatingStateProcessor(SecurityService securityService) {
    this.securityService = securityService;
  }

  @Override
  public void validateOperation(Object message, ProtobufSerializationService serializer,
      MessageExecutionContext messageContext, ProtobufOperationContext operationContext)
      throws ConnectionStateException {
    if (!(operationContext.getOperationHandler() instanceof HandshakeRequestOperationHandler)) {
      throw new ConnectionStateException(BasicTypes.ErrorCode.AUTHENTICATION_FAILED,
          "User has not yet authenticated");
    }
  }

  @Override
  public ProtobufConnectionAuthenticatingStateProcessor allowAuthentication()
      throws ConnectionStateException {
    return this;
  }

  public ProtobufConnectionStateProcessor authenticate(
      MessageExecutionContext messageExecutionContext, Properties properties)
      throws AuthenticationFailedException {
    Subject subject = securityService.login(properties);
    messageExecutionContext.setAuthorizer(new ShiroAuthorizer(securityService, subject));
    return new ProtobufConnectionAuthorizingStateProcessor();
  }
}
