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
package io.stargate.it.http;

import static java.lang.management.ManagementFactory.getRuntimeMXBean;

import io.stargate.it.exec.ProcessRunner;
import io.stargate.it.storage.*;
import java.io.File;
import java.lang.reflect.Method;
import java.util.*;
import java.util.Map.Entry;
import org.apache.commons.exec.CommandLine;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JUnit 5 extension for tests that need a REST API Service running in a separate process.
 *
 * <p>Note: this extension requires {@link ExternalStorage} and {@link StargateExtension} to be
 * activated as well. It is recommended that test classes be annotated with {@link
 * UseStargateCoordinator} to make sure both extensions are activated in the right order.
 *
 * <p>Note: this extension does not support concurrent test execution.
 *
 * @see RestApiSpec
 * @see RestApiParameters
 */
public class RestApiExtension extends ExternalResource<RestApiSpec, RestApiExtension.RestApiService>
    implements ParameterResolver {
  private static final Logger LOG = LoggerFactory.getLogger(RestApiExtension.class);

  public static final File LIB_DIR = initLibDir();

  public static final String STORE_KEY = "restapi-service";

  public static final String RESTAPI_STARTED_MESSAGE = "Started RestServiceServer";

  private static File initLibDir() {
    String dir = System.getProperty("stargate.rest.libdir");
    if (dir == null) {
      throw new IllegalStateException("stargate.rest.libdir system property is not set.");
    }

    return new File(dir);
  }

  private static File starterJar() {
    File[] files = LIB_DIR.listFiles();
    Assertions.assertNotNull(files, "No files in " + LIB_DIR.getAbsolutePath());
    return Arrays.stream(files)
        .filter(f -> f.getName().startsWith("sgv2-rest-service"))
        .filter(f -> f.getName().endsWith(".jar"))
        .findFirst()
        .orElseThrow(
            () ->
                new IllegalStateException(
                    "Unable to find REST Service jar in: " + LIB_DIR.getAbsolutePath()));
  }

  public RestApiExtension() {
    super(RestApiSpec.class, STORE_KEY, Namespace.GLOBAL);
  }

  private static RestApiParameters parameters(RestApiSpec spec, ExtensionContext context)
      throws Exception {
    RestApiParameters.Builder builder = RestApiParameters.builder();

    String customizer = spec.parametersCustomizer().trim();
    if (!customizer.isEmpty()) {
      Object testInstance = context.getTestInstance().orElse(null);
      Class<?> testClass = context.getRequiredTestClass();
      Method method = testClass.getMethod(customizer, RestApiParameters.Builder.class);
      method.invoke(testInstance, builder);
    }

    return builder.build();
  }

  @Override
  protected boolean isShared(RestApiSpec spec) {
    return spec.shared();
  }

  @Override
  protected Optional<RestApiService> processResource(
      RestApiService service, RestApiSpec spec, ExtensionContext context) throws Exception {
    StargateEnvironmentInfo stargateEnvironmentInfo =
        (StargateEnvironmentInfo)
            context.getStore(Namespace.GLOBAL).get(StargateExtension.STORE_KEY);
    Assertions.assertNotNull(
        stargateEnvironmentInfo,
        "Stargate coordinator is not available in " + context.getUniqueId());

    RestApiParameters params = parameters(spec, context);

    if (service != null) {
      if (service.matches(stargateEnvironmentInfo, spec, params)) {
        LOG.info("Reusing matching REST API Service {} for {}", spec, context.getUniqueId());
        return Optional.empty();
      }

      LOG.info(
          "Closing old REST API Service due to spec mismatch within {}", context.getUniqueId());
      service.close();
    }

    LOG.info("Starting REST API Service with spec {} for {}", spec, context.getUniqueId());

    RestApiService svc = new RestApiService(stargateEnvironmentInfo, spec, params);
    svc.start();
    return Optional.of(svc);
  }

  @Override
  public boolean supportsParameter(ParameterContext pc, ExtensionContext ec)
      throws ParameterResolutionException {
    return pc.getParameter().getType() == RestApiConnectionInfo.class;
  }

  @Override
  public Object resolveParameter(ParameterContext pc, ExtensionContext ec)
      throws ParameterResolutionException {
    return getResource(ec).orElseThrow(() -> new IllegalStateException("Cluster not available"));
  }

  protected static class RestApiService extends ExternalResource.Holder
      implements RestApiConnectionInfo, AutoCloseable {

    private final StargateEnvironmentInfo stargateEnvironmentInfo;
    private final RestApiSpec spec;
    private final RestApiParameters parameters;
    private final Instance instance;

    private RestApiService(
        StargateEnvironmentInfo stargateEnvironmentInfo,
        RestApiSpec spec,
        RestApiParameters parameters)
        throws Exception {
      this.stargateEnvironmentInfo = stargateEnvironmentInfo;
      this.spec = spec;
      this.parameters = parameters;

      instance = new Instance(stargateEnvironmentInfo, parameters);
    }

    private void start() {
      ShutdownHook.add(this);
      instance.start();
      instance.awaitReady();
    }

    private void stop() {
      ShutdownHook.remove(this);
      instance.stop();
      instance.awaitExit();
    }

    @Override
    public void close() {
      super.close();
      stop();
    }

    private boolean matches(
        StargateEnvironmentInfo stargateEnvironmentInfo,
        RestApiSpec spec,
        RestApiParameters parameters) {
      return this.stargateEnvironmentInfo.id().equals(stargateEnvironmentInfo.id())
          && this.spec.equals(spec)
          && this.parameters.equals(parameters);
    }

    @Override
    public String host() {
      return parameters.listenAddress();
    }

    @Override
    public int port() {
      return parameters.restPort();
    }

    @Override
    public int healthPort() {
      return parameters.metricsPort();
    }

    @Override
    public int metricsPort() {
      return parameters.metricsPort();
    }
  }

  private static class Instance extends ProcessRunner {

    private final CommandLine cmd;

    private Instance(StargateEnvironmentInfo stargateEnvironmentInfo, RestApiParameters params)
        throws Exception {
      super("RestAPI", 1, 1);

      cmd = new CommandLine("java");

      cmd.addArgument(
          "-Ddw.stargate.grpc.host=" + stargateEnvironmentInfo.nodes().get(0).seedAddress());
      cmd.addArgument("-Ddw.stargate.grpc.port=" + 8091);
      cmd.addArgument("-Ddw.server.connector.port=" + params.restPort());

      for (Entry<String, String> e : params.systemProperties().entrySet()) {
        cmd.addArgument("-D" + e.getKey() + "=" + e.getValue());
      }

      cmd.addArgument("-Dstargate.bridge.admin_token=mockAdminToken");

      if (isDebug()) {
        int debuggerPort = 5200;
        cmd.addArgument(
            "-agentlib:jdwp=transport=dt_socket,server=n,suspend=y,"
                + "address=localhost:"
                + debuggerPort);
      }

      cmd.addArgument("-jar");
      cmd.addArgument(starterJar().getAbsolutePath());

      addStdOutListener(
          (node, line) -> {
            if (line.contains(RESTAPI_STARTED_MESSAGE)) {
              ready();
            }
          });
    }

    private void start() {
      start(cmd, Collections.emptyMap());
    }

    private static boolean isDebug() {
      String args = getRuntimeMXBean().getInputArguments().toString();
      return args.contains("-agentlib:jdwp") || args.contains("-Xrunjdwp");
    }
  }
}
