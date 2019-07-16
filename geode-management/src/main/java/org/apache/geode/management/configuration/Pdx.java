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

package org.apache.geode.management.configuration;


import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.management.api.CorrespondWith;
import org.apache.geode.management.api.RestfulEndpoint;
import org.apache.geode.management.runtime.RuntimeInfo;

public class Pdx extends CacheElement implements RestfulEndpoint, CorrespondWith<RuntimeInfo> {
  public static final String PDX_ID = "PDX";
  public static final String PDX_ENDPOINT = "/configurations/pdx";

  protected Boolean readSerialized;
  protected ClassName pdxSerializer;
  protected Boolean ignoreUnreadFields;
  protected Boolean persistent;
  protected String diskStoreName;

  @Override
  @JsonIgnore
  public String getId() {
    return PDX_ID;
  }

  @Override
  public String getEndpoint() {
    return PDX_ENDPOINT;
  }

  @Override
  public String getIdentityEndPoint() {
    return PDX_ENDPOINT;
  }

  public void setGroup(String group) {
    if (group != null && !CLUSTER.equals(group)) {
      throw new IllegalArgumentException("Pdx can only be configured in cluster level.");
    }
  }

  public Boolean getReadSerialized() {
    return readSerialized;
  }

  public void setReadSerialized(Boolean readSerialized) {
    this.readSerialized = readSerialized;
  }

  public ClassName getPdxSerializer() {
    return pdxSerializer;
  }

  public void setPdxSerializer(ClassName pdxSerializer) {
    this.pdxSerializer = pdxSerializer;
  }

  public Boolean getIgnoreUnreadFields() {
    return ignoreUnreadFields;
  }

  public void setIgnoreUnreadFields(Boolean ignoreUnreadFields) {
    this.ignoreUnreadFields = ignoreUnreadFields;
  }

  public Boolean getPersistent() {
    return persistent;
  }

  public void setPersistent(Boolean persistent) {
    this.persistent = persistent;
  }

  public String getDiskStoreName() {
    return diskStoreName;
  }

  public void setDiskStoreName(String diskStoreName) {
    this.diskStoreName = diskStoreName;
  }
}
