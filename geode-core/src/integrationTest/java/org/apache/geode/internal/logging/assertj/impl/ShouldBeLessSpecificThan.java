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
package org.apache.geode.internal.logging.assertj.impl;

import org.apache.logging.log4j.Level;
import org.assertj.core.error.BasicErrorMessageFactory;
import org.assertj.core.error.ErrorMessageFactory;

public class ShouldBeLessSpecificThan extends BasicErrorMessageFactory {

  /**
   * Creates a new {@code ShouldBeLessSpecificThan}.
   *
   * @param actual the actual value in the failed assertion.
   * @param level the value that actual is being compared to in the failed assertion.
   * @return the created {@code ErrorMessageFactory}.
   */
  public static ErrorMessageFactory shouldBeLessSpecificThan(final Level actual,
      final Level level) {
    return new ShouldBeLessSpecificThan(actual, level);
  }

  private ShouldBeLessSpecificThan(final Level actual, final Level level) {
    super("%nExpecting:%n <%s>%nto be less specific than:%n <%s>%n", actual, level);
  }
}
