/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.it.exec;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteResultHandler;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.Executor;
import org.apache.commons.exec.LogOutputStream;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.exec.environment.EnvironmentUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessRunner {

  private static final Logger LOG = LoggerFactory.getLogger(ProcessRunner.class);

  private static final int PROCESS_WAIT_MINUTES =
      Integer.getInteger("stargate.test.process.wait.timeout.minutes", 10);

  protected final int generation;
  protected final int node;
  private final String kind;
  private final ExecuteWatchdog watchDog = new ExecuteWatchdog(ExecuteWatchdog.INFINITE_TIMEOUT);
  private final CompletableFuture<Void> ready = new CompletableFuture<>();
  private final CountDownLatch exit = new CountDownLatch(1);
  private final Collection<OutputListener> stdoutListeners = new ConcurrentLinkedQueue<>();

  public ProcessRunner(String kind, int generation, int node) {
    this.kind = kind;
    this.generation = generation;
    this.node = node;
  }

  public void addStdOutListener(OutputListener listener) {
    stdoutListeners.add(listener);
  }

  public void removeStdOutListener(OutputListener listener) {
    stdoutListeners.remove(listener);
  }

  protected void start(CommandLine cmd, Map<String, String> env) {
    String tag = kind.toLowerCase();

    LogOutputStream out =
        new LogOutputStream() {
          @Override
          protected void processLine(String line, int logLevel) {
            for (OutputListener listener : stdoutListeners) {
              listener.processLine(node, line);
            }

            LOG.info("{}{}-{}> {}", tag, generation, node, line);
          }
        };
    LogOutputStream err =
        new LogOutputStream() {
          @Override
          protected void processLine(String line, int logLevel) {
            LOG.error("{}{}-{}> {}", tag, generation, node, line);
          }
        };

    Executor executor =
        new DefaultExecutor() {
          @Override
          protected Thread createThread(Runnable runnable, String name) {
            return super.createThread(
                runnable, "process-runner-" + tag + "-" + generation + "-" + node);
          }
        };

    executor.setExitValues(new int[] {0, 143}); // normal exit, normal termination by SIGTERM
    executor.setStreamHandler(new PumpStreamHandler(out, err));
    executor.setWatchdog(watchDog);

    try {
      LOG.info("Starting {} {}, node {}: {}", kind, generation, node, cmd);

      Map<String, String> fullEnv = new HashMap<>(EnvironmentUtils.getProcEnvironment());
      fullEnv.putAll(env);

      executor.execute(cmd, fullEnv, new ExecutionCallback());

      LOG.info("Started {} {}, node {}: {}", kind, generation, node, cmd);
    } catch (IOException e) {
      LOG.info("Unable to start {} {}, node {}: {}", kind, generation, node, e.getMessage(), e);
    }
  }

  protected void ready() {
    LOG.info("{} {}/{} is ready", kind, generation, node);
    ready.complete(null);
  }

  public void stop() {
    LOG.info("Stopping {} process {}/{}", kind, generation, node);
    watchDog.destroyProcess();
    LOG.info("Signaled {} process {}/{} to stop", kind, generation, node);
  }

  protected void cleanup() {
    // nop by default
  }

  public void awaitReady() {
    try {
      ready.get(PROCESS_WAIT_MINUTES, TimeUnit.MINUTES);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  public void awaitExit() {
    try {
      if (!exit.await(PROCESS_WAIT_MINUTES, TimeUnit.MINUTES)) {
        throw new IllegalStateException(
            String.format("%s process %s/%s did not exit", kind, generation, node));
      }
    } catch (InterruptedException e) {
      throw new IllegalStateException(e);
    }
  }

  private class ExecutionCallback implements ExecuteResultHandler {

    @Override
    public void onProcessComplete(int exitValue) {
      LOG.info("{} process {}/{} exited with return code {}", kind, generation, node, exitValue);
      cleanup();
      ready.complete(null); // just in case
      exit.countDown();
    }

    @Override
    public void onProcessFailed(ExecuteException e) {
      LOG.info("{} process {}/{} failed with exception: {}", kind, generation, node, e);
      cleanup();
      ready.completeExceptionally(e);
      exit.countDown();
    }
  }
}
