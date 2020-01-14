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
package org.apache.geode.management.api;

import java.util.Objects;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.management.configuration.AbstractConfiguration;
import org.apache.geode.management.runtime.RuntimeInfo;

/**
 * @param <T> the type of the static config, e.g. RegionConfig
 * @param <R> the type of the corresponding runtime information, e.g. RuntimeRegionInfo
 */
@Experimental
public class ClusterManagementGetResult<T extends AbstractConfiguration<R>, R extends RuntimeInfo>
    extends ClusterManagementResult {
  private ConfigurationInfo<T, R> configurationInfo;

  public ClusterManagementGetResult() {}

  public ClusterManagementGetResult(ConfigurationInfo<T, R> configurationInfo) {
    this.configurationInfo = configurationInfo;
  }

  public ConfigurationInfo<T, R> getResult() {
    return configurationInfo;
  }

  public void setResult(ConfigurationInfo<T, R> configurationInfo) {
    this.configurationInfo = configurationInfo;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    ClusterManagementGetResult<?, ?> that = (ClusterManagementGetResult<?, ?>) o;
    return Objects.equals(configurationInfo, that.configurationInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), configurationInfo);
  }

  @Override
  public String toString() {
    return "ClusterManagementGetResult{" +
        "configurationInfo=" + configurationInfo +
        "} " + super.toString();
  }
}
