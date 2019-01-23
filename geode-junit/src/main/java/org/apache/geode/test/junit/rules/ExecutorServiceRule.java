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
package org.apache.geode.test.junit.rules;

import static java.lang.System.lineSeparator;

import java.lang.management.LockInfo;
import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.ref.WeakReference;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.apache.geode.test.junit.rules.serializable.SerializableExternalResource;

/**
 * Provides a reusable mechanism for executing tasks asynchronously in tests. This {@code Rule}
 * creates an {@code ExecutorService} which is terminated after the scope of the {@code Rule}. This
 * {@code Rule} can be used in tests for hangs, deadlocks, and infinite loops.
 *
 * <pre>
 * private CountDownLatch hangLatch = new CountDownLatch(1);
 *
 * {@literal @}Rule
 * public ExecutorServiceRule executorServiceRule = new ExecutorServiceRule();
 *
 * {@literal @}Test
 * public void doTest() throws Exception {
 *   Future<Void> result = executorServiceRule.runAsync(() -> {
 *     try {
 *       hangLatch.await();
 *     } catch (InterruptedException e) {
 *       throw new RuntimeException(e);
 *     }
 *   });
 *
 *   assertThatThrownBy(() -> result.get(1, MILLISECONDS)).isInstanceOf(TimeoutException.class);
 * }
 * </pre>
 *
 * <p>
 * The {@code Rule} can be configured to await termination by specifying
 * {@link Builder#awaitTermination(long, TimeUnit)}. If all tasks have not terminated by the
 * specified timeout, then {@code TimeoutException} will be thrown. This has the potential to
 * obscure any {@code Throwable}s thrown by the test itself.
 *
 * <p>
 * Example with awaitTermination enabled. Awaits up to timeout for all submitted tasks to terminate.
 * This causes the {@code Rule} to invoke awaitTermination during its tear down:
 *
 * <pre>
 * private CountDownLatch hangLatch = new CountDownLatch(1);
 *
 * {@literal @}Rule
 * public ExecutorServiceRule executorServiceRule = ExecutorServiceRule.builder().awaitTermination(10, SECONDS).build();
 *
 * {@literal @}Test
 * public void doTest() throws Exception {
 *   for (int i = 0; i < 10; i++) {
 *     executorServiceRule.runAsync(() -> {
 *       hangLatch.await();
 *     });
 *   }
 * }
 * </pre>
 */
@SuppressWarnings("unused")
public class ExecutorServiceRule extends SerializableExternalResource {

  protected final boolean enableAwaitTermination;
  protected final long awaitTerminationTimeout;
  protected final TimeUnit awaitTerminationTimeUnit;
  protected final boolean awaitTerminationBeforeShutdown;
  protected final boolean useShutdown;
  protected final boolean useShutdownNow;

  protected final CallStackFormatter callStackFormatter = new CallStackFormatter();

  protected transient volatile DedicatedThreadFactory threadFactory;
  protected transient volatile ExecutorService executor;

  /**
   * Returns a {@code Builder} to configure a new {@code ExecutorServiceRule}.
   */
  public static Builder builder() {
    return new Builder();
  }

  protected ExecutorServiceRule(Builder builder) {
    enableAwaitTermination = builder.enableAwaitTermination;
    awaitTerminationTimeout = builder.awaitTerminationTimeout;
    awaitTerminationTimeUnit = builder.awaitTerminationTimeUnit;
    awaitTerminationBeforeShutdown = builder.awaitTerminationBeforeShutdown;
    useShutdown = builder.useShutdown;
    useShutdownNow = builder.useShutdownNow;
  }

  /**
   * Constructs a {@code ExecutorServiceRule} which invokes {@code ExecutorService.shutdownNow()}
   * during {@code tearDown}.
   */
  public ExecutorServiceRule() {
    enableAwaitTermination = false;
    awaitTerminationTimeout = 0;
    awaitTerminationTimeUnit = TimeUnit.NANOSECONDS;
    awaitTerminationBeforeShutdown = false;
    useShutdown = false;
    useShutdownNow = true;
  }

  @Override
  public void before() {
    threadFactory = new DedicatedThreadFactory();
    executor = Executors.newCachedThreadPool(threadFactory);
  }

  @Override
  public void after() {
    if (awaitTerminationBeforeShutdown) {
      enableAwaitTermination();
    }
    if (useShutdown) {
      executor.shutdown();
    } else if (useShutdownNow) {
      executor.shutdownNow();
    }
    if (!awaitTerminationBeforeShutdown) {
      enableAwaitTermination();
    }
  }

  private void enableAwaitTermination() {
    if (enableAwaitTermination) {
      try {
        executor.awaitTermination(awaitTerminationTimeout, awaitTerminationTimeUnit);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Returns a direct reference to the underlying {@code ExecutorService}.
   */
  public ExecutorService getExecutorService() {
    return executor;
  }

  /**
   * Executes the given command at some time in the future.
   *
   * @param command the runnable task
   * @throws RejectedExecutionException if this task cannot be accepted for execution
   * @throws NullPointerException if command is null
   */
  public void execute(Runnable command) {
    executor.execute(command);
  }

  /**
   * Submits a value-returning task for execution and returns a Future representing the pending
   * results of the task. The Future's {@code get} method will return the task's result upon
   * successful completion.
   *
   * <p>
   * If you would like to immediately block waiting for a task, you can use constructions of the
   * form {@code result = exec.submit(aCallable).get();}
   *
   * @param task the task to submit
   * @param <T> the type of the task's result
   * @return a Future representing pending completion of the task
   * @throws RejectedExecutionException if the task cannot be scheduled for execution
   * @throws NullPointerException if the task is null
   */
  public <T> Future<T> submit(Callable<T> task) {
    return executor.submit(task);
  }

  /**
   * Submits a Runnable task for execution and returns a Future representing that task. The Future's
   * {@code get} method will return the given result upon successful completion.
   *
   * @param task the task to submit
   * @param result the result to return
   * @param <T> the type of the result
   * @return a Future representing pending completion of the task
   * @throws RejectedExecutionException if the task cannot be scheduled for execution
   * @throws NullPointerException if the task is null
   */
  public <T> Future<T> submit(Runnable task, T result) {
    return executor.submit(task, result);
  }

  /**
   * Submits a Runnable task for execution and returns a Future representing that task. The Future's
   * {@code get} method will return {@code null} upon <em>successful</em> completion.
   *
   * @param task the task to submit
   * @return a Future representing pending completion of the task
   * @throws RejectedExecutionException if the task cannot be scheduled for execution
   * @throws NullPointerException if the task is null
   */
  public Future<?> submit(Runnable task) {
    return executor.submit(task);
  }

  /**
   * Returns a new CompletableFuture that is asynchronously completed by a task running in the
   * dedicated executor after it runs the given action.
   *
   * @param runnable the action to run before completing the returned CompletableFuture
   * @return the new CompletableFuture
   */
  public CompletableFuture<Void> runAsync(Runnable runnable) {
    return CompletableFuture.runAsync(runnable, executor);
  }

  /**
   * Returns a new CompletableFuture that is asynchronously completed by a task running in the
   * dedicated executor with the value obtained by calling the given Supplier.
   *
   * @param supplier a function returning the value to be used to complete the returned
   *        CompletableFuture
   * @param <U> the function's return type
   * @return the new CompletableFuture
   */
  public <U> CompletableFuture<U> supplyAsync(Supplier<U> supplier) {
    return CompletableFuture.supplyAsync(supplier, executor);
  }

  /**
   * Returns the {@code Thread}s that are directly in the {@code ExecutorService}'s
   * {@code ThreadGroup} excluding subgroups.
   */
  public Set<Thread> getThreads() {
    return threadFactory.getThreads();
  }

  /**
   * Returns an array of {@code Thread Ids} that are directly in the {@code ExecutorService}'s
   * {@code ThreadGroup} excluding subgroups. {@code long[]} is returned to facilitate using JDK
   * APIs such as {@code ThreadMXBean#getThreadInfo(long[], int)}.
   */
  public long[] getThreadIds() {
    Set<Thread> threads = getThreads();
    long[] threadIds = new long[threads.size()];

    int i = 0;
    for (Thread thread : threads) {
      threadIds[i++] = thread.getId();
    }

    return threadIds;
  }

  /**
   * Returns formatted call stacks of the {@code Thread}s that are directly in the
   * {@code ExecutorService}'s {@code ThreadGroup} excluding subgroups.
   */
  public String dumpThreads() {
    StringBuilder dumpWriter = new StringBuilder();

    ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    ThreadInfo[] threadInfos = threadMXBean.getThreadInfo(getThreadIds(), true, true);

    for (ThreadInfo threadInfo : threadInfos) {
      if (threadInfo == null) {
        // sometimes ThreadMXBean.getThreadInfo returns array with one or more null elements
        continue;
      }
      callStackFormatter.formatThreadInfo(threadInfo, dumpWriter);
    }

    return dumpWriter.toString();
  }

  /**
   * Modified version of {@code java.util.concurrent.Executors$DefaultThreadFactory} that uses
   * a {@code Set<WeakReference<Thread>>} to track the {@code Thread}s in the factory's
   * {@code ThreadGroup} excluding subgroups.
   */
  protected static class DedicatedThreadFactory implements ThreadFactory {

    private static final AtomicInteger POOL_NUMBER = new AtomicInteger(1);

    private final ThreadGroup group;
    private final AtomicInteger threadNumber = new AtomicInteger(1);
    private final String namePrefix;
    private final Set<WeakReference<Thread>> directThreads = new HashSet<>();

    protected DedicatedThreadFactory() {
      group = new ThreadGroup(ExecutorServiceRule.class.getSimpleName() + "-ThreadGroup");
      namePrefix = "pool-" + POOL_NUMBER.getAndIncrement() + "-thread-";
    }

    @Override
    public Thread newThread(Runnable r) {
      Thread t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(), 0);
      if (t.isDaemon()) {
        t.setDaemon(false);
      }
      if (t.getPriority() != Thread.NORM_PRIORITY) {
        t.setPriority(Thread.NORM_PRIORITY);
      }
      directThreads.add(new WeakReference<>(t));
      return t;
    }

    protected Set<Thread> getThreads() {
      Set<Thread> value = new HashSet<>();
      for (WeakReference<Thread> reference : directThreads) {
        Thread thread = reference.get();
        if (thread != null) {
          value.add(thread);
        }
      }
      return value;
    }
  }

  /**
   * Copied from {@code OSProcess} and altered.
   */
  protected static class CallStackFormatter {

    private static final String TAB = "\t";
    private static final String DOUBLE_QUOTE = "\"";
    private static final String SINGLE_QUOTE = "\'";
    private static final String THREE_SPACES = "   ";
    private static final int MAX_CALL_STACK_FRAMES = 75;

    private final boolean formatHashCodeWidthTo16;

    protected CallStackFormatter() {
      formatHashCodeWidthTo16 = false;
    }

    protected void formatThreadInfo(ThreadInfo threadInfo, StringBuilder dumpBuilder) {
      dumpBuilder.append(DOUBLE_QUOTE).append(threadInfo.getThreadName()).append(DOUBLE_QUOTE);
      dumpBuilder.append(" tid=0x").append(Long.toHexString(threadInfo.getThreadId()));

      if (threadInfo.isSuspended()) {
        dumpBuilder.append(" (suspended)");
      }
      if (threadInfo.isInNative()) {
        dumpBuilder.append(" (in native)");
      }
      if (threadInfo.getLockOwnerName() != null) {
        dumpBuilder.append(" owned by ");
        dumpBuilder.append(DOUBLE_QUOTE).append(threadInfo.getLockOwnerName()).append(DOUBLE_QUOTE);
        dumpBuilder.append(" tid=0x").append(Long.toHexString(threadInfo.getLockOwnerId()));
      }

      LockInfo[] lockedSynchronizers = threadInfo.getLockedSynchronizers();
      if (lockedSynchronizers.length > 0) {
        for (LockInfo lockedSynchronizer : lockedSynchronizers) {
          dumpBuilder.append(lineSeparator());
          dumpBuilder.append(THREE_SPACES);
          dumpBuilder.append("- locked synchronizer ");
          dumpBuilder.append("<");
          dumpBuilder.append(formattedHexString(lockedSynchronizer.getIdentityHashCode()));
          dumpBuilder.append(">");
          dumpBuilder.append(" (a ").append(lockedSynchronizer.getClassName()).append(")");
        }
      }

      Thread.State threadState = threadInfo.getThreadState();

      dumpBuilder.append(lineSeparator());
      dumpBuilder.append(THREE_SPACES);
      dumpBuilder.append("java.lang.Thread.State: ").append(threadState);
      dumpBuilder.append(lineSeparator());

      int i = 0;
      StackTraceElement[] stackTrace = threadInfo.getStackTrace();

      LockInfo lockInfo = threadInfo.getLockInfo();
      MonitorInfo[] lockedMonitors = threadInfo.getLockedMonitors();

      for (; i < stackTrace.length && i < MAX_CALL_STACK_FRAMES; i++) {
        StackTraceElement stackTraceElement = stackTrace[i];

        dumpBuilder.append(TAB).append("at ").append(stackTraceElement);
        dumpBuilder.append(lineSeparator());

        if (i == 0 && lockInfo != null) {
          switch (threadState) {
            case BLOCKED:
              dumpBuilder.append(TAB);
              dumpBuilder.append("- waiting to lock ");
              dumpBuilder.append("<");
              dumpBuilder.append(formattedHexString(lockInfo.getIdentityHashCode()));
              dumpBuilder.append(">");
              dumpBuilder.append(" (a ").append(lockInfo.getClassName()).append(")");
              dumpBuilder.append(lineSeparator());
              break;
            case WAITING:
              dumpBuilder.append(TAB);
              dumpBuilder.append("- parking to wait for ");
              dumpBuilder.append("<");
              dumpBuilder.append(formattedHexString(lockInfo.getIdentityHashCode()));
              dumpBuilder.append(">");
              dumpBuilder.append(" (a ").append(lockInfo.getClassName()).append(")");
              dumpBuilder.append(lineSeparator());
              break;
            case TIMED_WAITING:
              dumpBuilder.append(TAB);
              dumpBuilder.append("- parking to timed-wait for ");
              dumpBuilder.append("<");
              dumpBuilder.append(formattedHexString(lockInfo.getIdentityHashCode()));
              dumpBuilder.append(">");
              dumpBuilder.append(" (a ").append(lockInfo.getClassName()).append(")");
              dumpBuilder.append(lineSeparator());
              break;
            default:
          }
        }

        for (MonitorInfo lockedMonitor : lockedMonitors) {
          if (lockedMonitor.getLockedStackDepth() == i) {
            dumpBuilder.append(TAB);
            dumpBuilder.append("- locked ");
            dumpBuilder.append("<");
            dumpBuilder.append(formattedHexString(lockedMonitor.getIdentityHashCode()));
            dumpBuilder.append(">");
            dumpBuilder.append(" (a ").append(lockedMonitor.getClassName()).append(")");
            dumpBuilder.append(lineSeparator());
          }
        }
      }

      if (i < stackTrace.length) {
        dumpBuilder.append(TAB);
        dumpBuilder.append("...");
        dumpBuilder.append(lineSeparator());
      }

      dumpBuilder.append(lineSeparator());
    }

    private String formattedHexString(int value) {
      if (formatHashCodeWidthTo16) {
        return String.format("0x%1$016x", value);
      } else {
        return String.format("0x%1$x", value);
      }
    }
  }

  public static class Builder {

    protected boolean enableAwaitTermination;
    protected long awaitTerminationTimeout;
    protected TimeUnit awaitTerminationTimeUnit = TimeUnit.NANOSECONDS;
    protected boolean awaitTerminationBeforeShutdown = true;
    protected boolean useShutdown;
    protected boolean useShutdownNow = true;

    protected Builder() {
      // nothing
    }

    /**
     * Enables invocation of {@code awaitTermination} during {@code tearDown}. Default is disabled.
     *
     * @param timeout the maximum time to wait
     * @param unit the time unit of the timeout argument
     */
    public Builder awaitTermination(long timeout, TimeUnit unit) {
      enableAwaitTermination = true;
      awaitTerminationTimeout = timeout;
      awaitTerminationTimeUnit = unit;
      return this;
    }

    /**
     * Enables invocation of {@code shutdown} during {@code tearDown}. Default is disabled.
     */
    public Builder useShutdown() {
      useShutdown = true;
      useShutdownNow = false;
      return this;
    }

    /**
     * Enables invocation of {@code shutdownNow} during {@code tearDown}. Default is enabled.
     *
     */
    public Builder useShutdownNow() {
      useShutdown = false;
      useShutdownNow = true;
      return this;
    }

    /**
     * Specifies invocation of {@code awaitTermination} before {@code shutdown} or
     * {@code shutdownNow}.
     */
    public Builder awaitTerminationBeforeShutdown() {
      awaitTerminationBeforeShutdown = true;
      return this;
    }

    /**
     * Specifies invocation of {@code awaitTermination} after {@code shutdown} or
     * {@code shutdownNow}.
     */
    public Builder awaitTerminationAfterShutdown() {
      awaitTerminationBeforeShutdown = false;
      return this;
    }

    /**
     * Builds the instance of {@code ExecutorServiceRule}.
     */
    public ExecutorServiceRule build() {
      return new ExecutorServiceRule(this);
    }
  }
}
