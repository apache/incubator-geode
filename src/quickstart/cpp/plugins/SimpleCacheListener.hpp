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

/*
 * SimpleCacheListener QuickStart Example.
 *
 * This is a simple implementation of a Cache Listener
 * It merely prints the events captured from the Geode Native Client.
 *
 */

// Include the Geode library.
#include <gfcpp/GeodeCppCache.hpp>
#include <gfcpp/CacheListener.hpp>

using namespace apache::geode::client;

// The SimpleCacheListener class.
class SimpleCacheListener : public CacheListener {
 public:
  // The Cache Listener callbacks.
  virtual void afterCreate(const EntryEvent& event);
  virtual void afterUpdate(const EntryEvent& event);
  virtual void afterInvalidate(const EntryEvent& event);
  virtual void afterDestroy(const EntryEvent& event);
  virtual void afterRegionInvalidate(const RegionEvent& event);
  virtual void afterRegionDestroy(const RegionEvent& event);
  virtual void close(const RegionPtr& region);
};
