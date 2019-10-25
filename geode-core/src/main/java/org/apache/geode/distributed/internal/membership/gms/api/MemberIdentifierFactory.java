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
package org.apache.geode.distributed.internal.membership.gms.api;

import java.util.Comparator;


/**
 * A MemberIdentifierFactory is provided when building a membership service. It must provide
 * implementations of the MemberIdentifier interface for use as identifiers in the membership
 * service. For Geode this implementation is InternalDistributedMember.<br>
 * See {@link MembershipBuilder} - where you inject your factory into GMS
 */
public interface MemberIdentifierFactory {
  /**
   * Create a new identifier instance
   */
  MemberIdentifier create(MemberData memberInfo);

  /**
   * Create a Comparator for the implementation of identifiers provided by this factory
   */
  Comparator<MemberIdentifier> getComparator();
}
