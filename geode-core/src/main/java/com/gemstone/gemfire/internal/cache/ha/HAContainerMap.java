/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.cache.ha;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientProxy;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * @since 5.7
 */
public class HAContainerMap implements HAContainerWrapper {

  /**
   * TODO: Amogh: Using ConcurrentHashMap may be beneficial. It gives us
   * putEntryIfAbsent()!
   */
  private Map map = null;

  /**
   * This map helps us retrieve the proxy id at the receiver side during GII so
   * that we can retain the cqlist of a client for an event which already
   * existed at the receiver side.
   */
  private final Map<String, CacheClientProxy> haRegionNameToProxy;

  public HAContainerMap(HashMap containerMap) {
    map = containerMap;
    haRegionNameToProxy = new ConcurrentHashMap<String, CacheClientProxy>();
  }

  public ClientProxyMembershipID getProxyID(String haRegionName) {
    CacheClientProxy proxy = haRegionNameToProxy.get(haRegionName);
    if (proxy != null){
      return proxy.getProxyID();
    } else {
      return null;
    }
  }

  public Object putProxy(String haName, CacheClientProxy proxy) {
//    InternalDistributedSystem.getLoggerI18n().info(LocalizedStrings.DEBUG, "adding proxy for " + haName + ": " + proxy, new Exception("stack trace"));
    return haRegionNameToProxy.put(haName, proxy);
  }

  public CacheClientProxy getProxy(String haRegionName) {
    return haRegionNameToProxy.get(haRegionName);
  }

  public Object removeProxy(String haName) {
//    InternalDistributedSystem.getLoggerI18n().info(LocalizedStrings.DEBUG, "removing proxy for " + haName, new Exception("stack trace"));
    return haRegionNameToProxy.remove(haName);
  }
  
  /**
   * @param key
   * @return Object
   */
  public Object getKey(Object key) {
    synchronized (map) {
      Entry entry = (Entry)map.get(key);
      return (entry == null) ? null : entry.getKey();
    }
  }

  public String getName() {
    return "HashMap";
  }

  public void cleanUp() {
    // TODO: Amogh: Should we make the map instance null?
    clear();
  }

  public void clear() {
    synchronized (map) {
      map.clear();
    }
  }

  public boolean containsKey(Object key) {
    synchronized (map) {
      return map.containsKey(key);
    }
  }

  public boolean containsValue(Object value) {
    //return map.containsValue(value);
    throw new UnsupportedOperationException("containsValue() not supported.");
  }

  public Set entrySet() {
    //return map.entrySet();
    throw new UnsupportedOperationException("entrySet() not supported.");
  }

  public Object get(Object key) {
    synchronized (map) {
      Entry entry = (Entry)map.get(key);
      return (entry == null) ? null : entry.getValue();
    }
  }

  public Object getEntry(Object key) {
    synchronized (map) {
      return map.get(key);
    }
  }

  public boolean isEmpty() {
    synchronized (map) {
      return map.isEmpty();
    }
  }

  public Set keySet() {
    synchronized (map) {
      return map.keySet();
    }
  }

  public Object put(Object key, Object value) {
    Entry entry = new Entry(key, value);
    synchronized (map) {
      return map.put(key, entry);
    }
  }

  public void putAll(Map t) {
    //synchronized (map) {
    //  map.putAll(t);
    //}
    throw new UnsupportedOperationException("putAll() not supported.");
  }

  public Object remove(Object key) {
    synchronized (map) {
      Entry entry = (Entry)map.remove(key);
      return (entry == null) ? null : entry.getValue();
    }
  }

  public int size() {
    synchronized (map) {
      return map.size();
    }
  }

  public Collection values() {
    //return map.values();
    throw new UnsupportedOperationException("values() not supported.");
  }

  static protected class Entry implements Map.Entry {
    private Object key = null;

    private Object value = null;

    public Entry(Object key, Object val) {
      if (key == null || val == null) {
        throw new IllegalArgumentException("key or value cannot be null.");
      }
      this.key = key;
      this.value = val;
    }

    public Object getKey() {
      return this.key;
    }

    public Object getValue() {
      return this.value;
    }

    public Object setValue(Object val) {
      throw new UnsupportedOperationException("setValue() not supported.");
    }
  }

}
