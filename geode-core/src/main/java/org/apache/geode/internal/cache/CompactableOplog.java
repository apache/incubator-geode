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
package org.apache.geode.internal.cache;

import org.apache.geode.internal.cache.DiskStoreImpl.OplogCompactor;
import org.apache.geode.internal.cache.persistence.BytesAndBits;
import org.apache.geode.internal.cache.persistence.DiskRegionView;

/**
 * Contract that must be implemented by oplogs so that they can be compacted.
 *
 *
 * @since GemFire 6.5
 */

public interface CompactableOplog {
  public void prepareForCompact();

  public int compact(OplogCompactor compactor);

  public BytesAndBits getBytesAndBits(DiskRegionView dr, DiskId id, boolean faultIn,
      boolean bitOnly);

  public BytesAndBits getNoBuffer(DiskRegion dr, DiskId id);
}
