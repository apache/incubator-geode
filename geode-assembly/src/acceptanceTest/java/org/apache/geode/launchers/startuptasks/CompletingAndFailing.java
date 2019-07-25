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

package org.apache.geode.launchers.startuptasks;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.ServerLauncher;
import org.apache.geode.distributed.ServerLauncherCacheProvider;
import org.apache.geode.internal.cache.InternalCache;

public class CompletingAndFailing implements ServerLauncherCacheProvider {

  public static final Exception EXCEPTION = new IllegalStateException("Startup task failed");

  @Override
  public Cache createCache(Properties gemfireProperties, ServerLauncher serverLauncher) {
    final CacheFactory cacheFactory = new CacheFactory(gemfireProperties);

    InternalCache cache = (InternalCache) cacheFactory.create();

    CompletableFuture<Void> completingStartupTask = CompletableFuture.completedFuture(null);

    CompletableFuture<Void> failingStartupTask = new CompletableFuture<>();
    failingStartupTask.completeExceptionally(EXCEPTION);

    cache.getInternalResourceManager().addStartupTask(completingStartupTask);
    cache.getInternalResourceManager().addStartupTask(failingStartupTask);
    return cache;
  }
}
