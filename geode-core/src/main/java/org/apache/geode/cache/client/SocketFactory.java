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

package org.apache.geode.cache.client;

import java.io.IOException;
import java.net.Socket;

/**
 * A socket factory used to create sockets from a client to locators or servers.
 *
 * @see PoolFactory#setSocketFactory(SocketFactory)
 */
public interface SocketFactory {

  /**
   * The default socket factory, which just calls {@link Socket#Socket()}
   */
  SocketFactory DEFAULT = Socket::new;

  /**
   * Create a (unconnected) tcp socket for establishing a client.
   *
   * @return an unconnected socket
   */
  Socket createSocket() throws IOException;
}
