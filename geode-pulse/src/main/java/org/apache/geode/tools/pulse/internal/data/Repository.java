/*
 *
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

package org.apache.geode.tools.pulse.internal.data;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.ResourceBundle;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.client.OAuth2AuthorizedClient;
import org.springframework.security.oauth2.client.OAuth2AuthorizedClientService;
import org.springframework.security.oauth2.client.authentication.OAuth2AuthenticationToken;

/**
 * A Singleton instance of the memory cache for clusters.
 *
 * @since GemFire version 7.0.Beta 2012-09-23
 */
public class Repository {
  private static final Logger logger = LogManager.getLogger();

  private static Repository instance = new Repository();
  private final HashMap<String, Cluster> clusterMap = new HashMap<>();
  private Boolean jmxUseLocator;
  private String host;
  private String port;
  private boolean useSSLLocator = false;
  private boolean useSSLManager = false;
  private Properties javaSslProperties;
  private ApplicationContext applicationContext;

  Locale locale =
      new Locale(PulseConstants.APPLICATION_LANGUAGE, PulseConstants.APPLICATION_COUNTRY);

  private ResourceBundle resourceBundle =
      ResourceBundle.getBundle(PulseConstants.LOG_MESSAGES_FILE, locale);

  private PulseConfig pulseConfig = new PulseConfig();

  private Repository() {}

  public static Repository get() {
    return instance;
  }

  public Boolean getJmxUseLocator() {
    return this.jmxUseLocator;
  }

  public void setJmxUseLocator(Boolean jmxUseLocator) {
    this.jmxUseLocator = jmxUseLocator;
  }

  public String getHost() {
    return this.host;
  }

  public void setHost(String jmxHost) {
    this.host = jmxHost;
  }

  public String getPort() {
    return this.port;
  }

  public void setPort(String jmxPort) {
    this.port = jmxPort;
  }

  public boolean isUseSSLLocator() {
    return useSSLLocator;
  }

  public void setUseSSLLocator(boolean useSSLLocator) {
    this.useSSLLocator = useSSLLocator;
  }

  public boolean isUseSSLManager() {
    return useSSLManager;
  }

  public void setUseSSLManager(boolean useSSLManager) {
    this.useSSLManager = useSSLManager;
  }

  public PulseConfig getPulseConfig() {
    return this.pulseConfig;
  }

  public Properties getJavaSslProperties() {
    return javaSslProperties;
  }

  public void setJavaSslProperties(Properties javaSslProperties) {
    this.javaSslProperties = javaSslProperties;
  }

  /**
   * this will return a cluster already connected to the geode jmx manager for the user in the
   * request
   *
   * But for multi-user connections to gemfireJMX, i.e pulse that uses gemfire integrated security,
   * we will need to get the username from the context
   */
  public Cluster getCluster() {
    Authentication auth = SecurityContextHolder.getContext().getAuthentication();
    if (auth == null)
      return null;

    if (auth instanceof OAuth2AuthenticationToken) {
      OAuth2AuthenticationToken token = (OAuth2AuthenticationToken) auth;
      OAuth2AuthorizedClientService authorizedClientService =
          applicationContext.getBean(OAuth2AuthorizedClientService.class);
      OAuth2AuthorizedClient client = authorizedClientService.loadAuthorizedClient(
          token.getAuthorizedClientRegistrationId(),
          token.getName());

      return getClusterWithCredentials(token.getPrincipal().getName(),
          client.getAccessToken().getTokenValue());
    }

    return getClusterWithUserNameAndPassword(auth.getName(), null);
  }

  public Cluster getClusterWithUserNameAndPassword(String userName, String password) {
    return getClusterWithCredentials(userName, new String[] {userName, password});
  }

  public Cluster getClusterWithCredentials(String username, Object credentials) {
    synchronized (this.clusterMap) {
      Cluster data = clusterMap.get(username);
      if (data == null) {
        logger.info(resourceBundle.getString("LOG_MSG_CREATE_NEW_THREAD") + " : " + username);
        data = new Cluster(this.host, this.port, username);
        // Assign name to thread created
        data.setName(PulseConstants.APP_NAME + "-" + this.host + ":" + this.port + ":" + username);
        data.connectToGemFire(credentials);
        if (data.isConnectedFlag()) {
          this.clusterMap.put(username, data);
        }
      }
      return data;
    }
  }

  public void logoutUser(String username) {
    Cluster data = clusterMap.remove(username);
    if (data != null) {
      try {
        data.setStopUpdates(true);
        data.getJMXConnector().close();
      } catch (Exception e) {
        // We're logging out so this can be ignored
      }
    }
  }

  // This method is used to remove all cluster threads
  public void removeAllClusters() {

    Iterator<Map.Entry<String, Cluster>> iter = clusterMap.entrySet().iterator();

    while (iter.hasNext()) {
      Map.Entry<String, Cluster> entry = iter.next();
      Cluster c = entry.getValue();
      String clusterKey = entry.getKey();
      c.stopThread();
      iter.remove();
      logger.info("{} : {}", resourceBundle.getString("LOG_MSG_REMOVE_THREAD"), clusterKey);
    }
  }

  public ResourceBundle getResourceBundle() {
    return this.resourceBundle;
  }

  public void setApplicationContext(ApplicationContext applicationContext) {
    this.applicationContext = applicationContext;
  }
}
