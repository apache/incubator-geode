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

package org.apache.geode.internal.statistics.platform;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.CancelCriterion;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.internal.statistics.LocalStatisticsFactory;
import org.apache.geode.internal.statistics.SimpleStatSampler;
import org.apache.geode.internal.statistics.platform.LinuxThreadSchedulerStatistics.WorkingGatherer;

public class LinuxThreadSchedulerStatisticsTest {

  private static final Path NON_EXISTENT_FILE_PATH =
      Paths.get("this-file-does-not-exist-EpCAd9DKL3TF");
  private static final URL SCHEDSTAT_UNKNOWN_VERSION_URL =
      LinuxThreadSchedulerStatisticsTest.class.getResource("schedstat-version-16.txt");
  private static final URL SCHEDSTAT_VALID_URL =
      LinuxThreadSchedulerStatisticsTest.class.getResource("schedstat-valid-1.txt");

  StatisticsFactory statisticsFactory;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  private File testDir;

  @Before
  public void before() {

    testDir = temporaryFolder.getRoot();

    /*
     * Put statistics files (generated indirectly via LocalStatisticsFactory) in temporary
     * folder so they don't intermingle with our source code.
     */
    System.setProperty(SimpleStatSampler.ARCHIVE_FILE_NAME_PROPERTY, this.testDir.getAbsolutePath()
        + File.separator + SimpleStatSampler.DEFAULT_ARCHIVE_FILE_NAME);

    statisticsFactory = new LocalStatisticsFactory(new CancelCriterion() {
      @Override
      public String cancelInProgress() {
        return null;
      }

      @Override
      public RuntimeException generateCancelledException(final Throwable throwable) {
        return null;
      }
    });
  }

  @Test
  public void createsNoOpSamplerForNonExistentSchedstatFile() {
    final LinuxThreadSchedulerStatistics.Gatherer gatherer =
        LinuxThreadSchedulerStatistics
            .create(statisticsFactory, statisticsFactory::createAtomicStatistics,
                NON_EXISTENT_FILE_PATH);
    /*
     * this assertion assumes that WorkingSampler is the only one
     * working implementation of the Sampler interface
     */
    assertThat(gatherer).isNotInstanceOf(LinuxThreadSchedulerStatistics.WorkingGatherer.class);
    gatherer.sample(); // verify we don't throw an exception from sample()
  }

  @Test
  public void ignoresUnknownVersion() throws URISyntaxException {
    final WorkingGatherer sampler =
        workingSamplerFor(SCHEDSTAT_UNKNOWN_VERSION_URL);
    sampler.sample();
    final Statistics cpuStatistics = sampler.statisticsForCpuIds.get("cpu0");
    assertThat(cpuStatistics).isNull();
  }

  @Test
  public void processesCorrectInput() throws URISyntaxException {
    final WorkingGatherer sampler =
        workingSamplerFor(SCHEDSTAT_VALID_URL);
    sampler.sample();
    validateStatistics(sampler, "cpu0", 15690060435L, 31061208584L, 46293L, 670969.878469747);
    validateStatistics(sampler, "cpu1", 25690060435L, 41061208584L, 56293L, 729419.4408541026);
  }

  @Test
  public void ignoresDomainStatistics() throws URISyntaxException {
    final WorkingGatherer sampler =
        workingSamplerFor(SCHEDSTAT_VALID_URL);
    sampler.sample();
    final Statistics domain0Statistics = sampler.statisticsForCpuIds.get("domain0");
    assertThat(domain0Statistics).isNull();
  }

  private LinuxThreadSchedulerStatistics.WorkingGatherer workingSamplerFor(final URL schedstatURL)
      throws URISyntaxException {
    return new LinuxThreadSchedulerStatistics.WorkingGatherer(statisticsFactory,
        statisticsFactory::createAtomicStatistics,
        Paths.get(schedstatURL.toURI()));
  }

  private void validateStatistics(final WorkingGatherer sampler, final String cpuId,
      final long runningTimeNanos, final long queuedTimeNanos,
      final long tasksScheduledCount,
      final double meanTaskQueuedTimeNanos) {
    final Statistics cpuStatistics = sampler.statisticsForCpuIds.get(cpuId);
    assertThat(cpuStatistics.getLong(sampler.runningTimeNanosId)).isEqualTo(runningTimeNanos);
    assertThat(cpuStatistics.getLong(sampler.queuedTimeNanosId)).isEqualTo(queuedTimeNanos);
    assertThat(cpuStatistics.getLong(sampler.tasksScheduledCountId))
        .isEqualTo(tasksScheduledCount);
    assertThat(cpuStatistics.getDouble(sampler.meanTaskQueuedTimeNanosId)).isEqualTo(
        meanTaskQueuedTimeNanos);
  }
}
