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
package org.apache.geode.protocol.protobuf.operations;

import java.util.Set;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.protocol.operations.OperationHandler;
import org.apache.geode.protocol.protobuf.RegionAPI;
import org.apache.geode.protocol.protobuf.Result;
import org.apache.geode.protocol.protobuf.Success;
import org.apache.geode.protocol.protobuf.utilities.ProtobufResponseUtilities;
import org.apache.geode.serialization.SerializationService;

@Experimental
public class GetRegionNamesRequestOperationHandler
    implements OperationHandler<RegionAPI.GetRegionNamesRequest, RegionAPI.GetRegionNamesResponse> {

  @Override
  public Result<RegionAPI.GetRegionNamesResponse> process(SerializationService serializationService,
      RegionAPI.GetRegionNamesRequest request, Cache cache) {
    Set<Region<?, ?>> regions = cache.rootRegions();
    return Success.of(ProtobufResponseUtilities.createGetRegionNamesResponse(regions));
  }
}
