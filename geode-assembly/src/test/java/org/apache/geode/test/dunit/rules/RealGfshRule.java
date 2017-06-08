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
package org.apache.geode.test.dunit.rules;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Rule;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class RealGfshRule extends ExternalResource {
  private TemporaryFolder temporaryFolder = new TemporaryFolder();
  private List<Process> processes = new ArrayList<>();
  private final String gfshPath;

  public RealGfshRule(String gfshPath) {
    this.gfshPath = gfshPath;
  }

  @Override
  protected void before() throws IOException {
    temporaryFolder.create();
  }

  public Process executeCommands(String... commands) throws IOException {
    String[] gfshCommands = new String[commands.length + 1];
    gfshCommands[0] = gfshPath;

    for (int i = 0; i < commands.length; i++) {
      gfshCommands[i + 1] = "-e " + commands[i];
    }

    ProcessBuilder processBuilder =
        new ProcessBuilder(gfshCommands).inheritIO().directory(temporaryFolder.getRoot());

    Process process = processBuilder.start();
    processes.add(process);

    return process;
  }

  public Process executeCommandsAndWaitFor(int timeout, TimeUnit timeUnit, String... commands)
      throws IOException, InterruptedException {
    Process process = executeCommands(commands);
    assertThat(process.waitFor(timeout, timeUnit)).isTrue();
//    assertThat(process.exitValue()).isEqualTo(0);

    return process;
  }

  @Override
  protected void after() {
    System.out.println("Cleaning up");
    try {
      executeCommandsAndWaitFor(1, TimeUnit.MINUTES, "connect", "stop locator --dir=" + temporaryFolder.getRoot().getAbsolutePath());
    } catch (Exception ignore) {
    }
    processes.stream().forEach(Process::destroyForcibly);
    processes.stream().forEach((Process process) -> {
      try {
        process.waitFor(1, TimeUnit.MINUTES);
      } catch (Exception ignore) {
      }
    });
    temporaryFolder.delete();
  }
}
