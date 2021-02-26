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
 *
 */

package org.apache.geode.management.internal.configuration;


import static java.util.stream.Collectors.toSet;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE_SIZE_LIMIT;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.Serializable;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;

import org.apache.geode.cache.Cache;
import org.apache.geode.deployment.internal.JarDeploymentServiceFactory;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.classloader.ClassPathLoader;
import org.apache.geode.management.configuration.Deployment;
import org.apache.geode.management.internal.configuration.domain.Configuration;
import org.apache.geode.management.internal.utils.JarFileUtils;
import org.apache.geode.services.result.ServiceResult;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

public class ClusterConfig implements Serializable {
  private List<ConfigGroup> groups;

  public ClusterConfig(ConfigGroup... configGroups) {
    this.groups = new ArrayList<>();

    Collections.addAll(this.groups, configGroups);
  }

  private static Set<String> toSetIgnoringHiddenFiles(String[] array) {
    if (array == null) {
      return Collections.emptySet();
    }
    return Arrays.stream(array).filter((String name) -> !name.startsWith(".")).collect(toSet());
  }

  private static String nameOfClassContainedInJar(String deploymentName) {
    switch (deploymentName) {
      case "cluster":
        return "Cluster";
      case "group1":
        return "Group1";
      case "group2":
        return "Group2";
      default:
        throw new IllegalArgumentException(
            "We don't know what class to expect in the jar named " + deploymentName);
    }
  }

  public Set<String> getMaxLogFileSizes() {
    if (this.groups.size() == 0) {
      return Collections.emptySet();
    }
    return this.groups.stream().map(ConfigGroup::getMaxLogFileSize).filter(Objects::nonNull)
        .collect(toSet());
  }

  public List<String> getDeploymentNames() {
    return groups.stream()
        .flatMap((ConfigGroup configGroup) -> configGroup.getDeploymentNames().stream())
        .collect(Collectors.toList());
  }

  public List<String> getRegions() {
    return groups.stream().flatMap((ConfigGroup configGroup) -> configGroup.getRegions().stream())
        .collect(Collectors.toList());
  }

  public List<ConfigGroup> getGroups() {
    return Collections.unmodifiableList(groups);
  }

  public void verify(MemberVM memberVM) throws ClassNotFoundException {
    if (memberVM.isLocator()) {
      verifyLocator(memberVM);
    } else {
      verifyServer(memberVM);
    }
  }

  public void verifyLocator(MemberVM locatorVM) {
    Set<String> expectedGroupConfigs =
        this.getGroups().stream().map(ConfigGroup::getName).collect(toSet());

    // verify info exists in memory
    locatorVM.invoke(() -> {
      InternalLocator internalLocator = ClusterStartupRule.getLocator();
      InternalConfigurationPersistenceService sc =
          internalLocator.getConfigurationPersistenceService();

      // verify no extra configs exist in memory
      Set<String> actualGroupConfigs = sc.getConfigurationRegion().keySet();
      assertThat(actualGroupConfigs).isEqualTo(expectedGroupConfigs);

      for (ConfigGroup configGroup : this.getGroups()) {
        // verify jars are as expected
        Configuration config = sc.getConfiguration(configGroup.name);
        Set<String> deploymentNames =
            config.getDeployments().stream().map(Deployment::getDeploymentName).collect(toSet());
        assertThat(deploymentNames).isEqualTo(configGroup.getDeploymentNames());

        // verify property is as expected
        if (StringUtils.isNotBlank(configGroup.getMaxLogFileSize())) {
          Properties props = config.getGemfireProperties();
          assertThat(props.getProperty(LOG_FILE_SIZE_LIMIT))
              .isEqualTo(configGroup.getMaxLogFileSize());
        }

        // verify region is in the region xml
        for (String regionName : configGroup.getRegions()) {
          String regionXml = "<region name=\"" + regionName + "\"";
          assertThat(config.getCacheXmlContent()).contains(regionXml);
        }
      }


    });

    File clusterConfigDir = new File(locatorVM.getWorkingDir(), "/cluster_config");

    for (ConfigGroup configGroup : this.getGroups()) {
      Set<String> actualFiles =
          toSetIgnoringHiddenFiles(new File(clusterConfigDir, configGroup.name).list()).stream()
              .map(JarFileUtils::getArtifactId).collect(toSet());

      Set<String> expectedDeployments = configGroup.getDeploymentNames();
      assertThat(actualFiles).isEqualTo(expectedDeployments);
    }
  }

  public void verifyServer(MemberVM serverVM) {
    // verify files exist in filesystem
    Set<String> expectedJarNames = new HashSet<>(this.getDeploymentNames());

    String[] actualJarFiles =
        serverVM.getWorkingDir().list((dir, filename) -> filename.contains(".jar"));
    Set<String> actualJarNames = Stream.of(actualJarFiles)
        .map(jar -> JarFileUtils.getArtifactId(jar.replaceAll("\\.v\\d+\\.jar", ".jar")))
        .collect(toSet());

    // We will end up with extra jars on disk if they are deployed and then undeployed
    assertThat(expectedJarNames).isSubsetOf(actualJarNames);

    // verify config exists in memory
    serverVM.invoke(() -> {
      Cache cache = GemFireCacheImpl.getInstance();

      // TODO: set compare to fail if there are extra regions
      for (String region : this.getRegions()) {
        assertThat(cache.getRegion(region)).isNotNull();
      }

      if (this.getMaxLogFileSizes().size() > 0) {
        Properties props = cache.getDistributedSystem().getProperties();
        assertThat(this.getMaxLogFileSizes()).contains(props.getProperty(LOG_FILE_SIZE_LIMIT));
      }

      for (String deploymentName : this.getDeploymentNames()) {
        ServiceResult<Deployment> serviceResult =
            ClassPathLoader.getLatest().getJarDeploymentService()
                .getDeployed(deploymentName);
        assertThat(serviceResult.isSuccessful()).isTrue();
        Deployment deployment = serviceResult.getMessage();
        assertThat(Class.forName(nameOfClassContainedInJar(deploymentName), true,
            new URLClassLoader(new URL[] {deployment.getFile().toURI().toURL()}))).isNotNull();
      }

      // If we have extra jars on disk left over from undeploy, make sure they
      // aren't used
      Set<String> undeployedJarNames = new HashSet<>(actualJarNames);
      undeployedJarNames.removeAll(expectedJarNames);
      for (String jar : undeployedJarNames) {
        System.out.println("Verifying undeployed jar: " + jar);

        ServiceResult<Deployment> serviceResult =
            ClassPathLoader.getLatest().getJarDeploymentService().getDeployed(jar);
        assertThat(serviceResult.isFailure()).isTrue();
      }
    });
  }
}
