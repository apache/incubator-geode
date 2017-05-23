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
package org.apache.geode.management.internal;

import javax.management.Notification;
import javax.management.ObjectName;

import org.apache.geode.distributed.internal.MembershipListener;

/**
 * 
 * A listener to handle MBean proxy related events.
 * 
 * Any custom ProxyListener should implement this interface. For the time being its not exposed to
 * the user. But is being kept as an extension point for future releases if we decide to support
 * custom ProxyListener.
 * 
 * TODO Along side we need to make FederationCompoent public as well.
 * 
 * A simple usage of a ProxyListener could be custom aggregator of Statistics exposed by MBeans.
 * 
 * 
 * ManagementService service = ManagementService .getManagementService(cache);
 * 
 * ProxyListener listener = new MyListener(); //implements ProxyListener.
 * service.addProxyListener(listener)
 * 
 * 
 * Adding an ProxyListener in a non Manager node wont be useful as it wont get any proxy related
 * callbacks
 * 
 * 
 */
public interface ProxyListener extends MembershipListener {

  /**
   * 
   * When a new proxy is added to the Manager this call back is invoked. User can decide what to do
   * with the event. It can take some action or ignore the event.
   * 
   * @param objectName name of the proxy object
   * @param interfaceClass interface class of the proxy object.
   * @param proxyObject actual reference of the proxy. This proxy object can be cast to the input
   *        interface class type.
   * @param newVal new value for the proxy
   * 
   */
  void afterCreateProxy(ObjectName objectName, Class interfaceClass, Object proxyObject,
      FederationComponent newVal);

  /**
   * 
   * When a proxy is removed from the Manager this call back is invoked. User can decide what to do
   * with the event. It can take some action or ignore the event.
   * 
   * @param objectName name of the proxy object
   * @param interfaceClass interface class of the proxy object.
   * @param proxyObject actual reference of the proxy.
   * @param oldVal old value for the proxy
   */
  void afterRemoveProxy(ObjectName objectName, Class interfaceClass, Object proxyObject,
      FederationComponent oldVal);

  /**
   * When a proxy is updated in Manager this call back is invoked. User can decide what to do with
   * the event. It can take some action or ignore the event.
   * 
   * @param objectName name of the proxy object
   * @param interfaceClass interface class of the proxy object.
   * @param proxyObject actual reference of the proxy.
   * @param newVal new value for the proxy
   * @param oldVal old value for the proxy
   */
  void afterUpdateProxy(ObjectName objectName, Class interfaceClass, Object proxyObject,
      FederationComponent newVal, FederationComponent oldVal);

  /**
   * User can implement this method to handle all notifications generated by the System.
   * 
   * @param notification
   */
  void handleNotification(Notification notification);

  /**
   * This is a very special method, Only to be invoked from MonitoringRegionCacheListener. The need
   * for this interface arises as some time a create op is not meant to create proxies or aggregate.
   * But only to feed data to existing proxies.
   * 
   * e.g. When manager starts and not a single ManagementTask cycle has been run. This will result
   * in lost updates. To capture all updates we can feed the newly created data to existing
   * aggregate.
   * 
   * @param objectName name of the proxy object
   * @param interfaceClass interface class of the proxy object.
   * @param proxyObject actual reference of the proxy. This proxy object can be cast to the input
   *        interface class type.
   * @param newVal new value for the proxy
   */
  void afterPseudoCreateProxy(ObjectName objectName, Class interfaceClass, Object proxyObject,
      FederationComponent newVal);

}
