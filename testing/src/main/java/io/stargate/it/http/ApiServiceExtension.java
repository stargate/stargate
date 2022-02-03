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
 * JUnit 5 extension for tests that need an API Service running in a separate process.
 *
 * <p>Note: this extension requires {@link ExternalStorage} and {@link StargateExtension} to be
 * activated as well. It is recommended that test classes be annotated with {@link
 * UseStargateCoordinator} to make sure both extensions are activated in the right order.
 *
 * @see ApiServiceSpec
 * @see ApiServiceParameters
 */
public class ApiServiceExtension
    extends ExternalResource<ApiServiceSpec, ApiServiceExtension.ApiService>
    implements ParameterResolver {
  private static final Logger LOG = LoggerFactory.getLogger(ApiServiceExtension.class);

  // TODO - move these into environment info
  private static String BRIDGE_PORT = "8091";
  private static String BRIDGE_TOKEN = "mockAdminToken";

  public static final String STORE_KEY = "api-container";

  public ApiServiceExtension() {
    super(ApiServiceSpec.class, STORE_KEY, Namespace.GLOBAL);
  }

  private static ApiServiceParameters parameters(ApiServiceSpec spec, ExtensionContext context)
      throws Exception {
    ApiServiceParameters.Builder builder = ApiServiceParameters.builder();

    String customizer = spec.parametersCustomizer().trim();
    if (!customizer.isEmpty()) {
      Object testInstance = context.getTestInstance().orElse(null);
      Class<?> testClass = context.getRequiredTestClass();
      Method method = testClass.getMethod(customizer, ApiServiceParameters.Builder.class);
      method.invoke(testInstance, builder);
    }

    return builder.build();
  }

  @Override
  protected boolean isShared(ApiServiceSpec spec) {
    return spec.shared();
  }

  @Override
  protected Optional<ApiService> processResource(
      ApiService service, ApiServiceSpec spec, ExtensionContext context) throws Exception {
    StargateEnvironmentInfo stargateEnvironmentInfo =
        (StargateEnvironmentInfo)
            context.getStore(Namespace.GLOBAL).get(StargateExtension.STORE_KEY);
    Assertions.assertNotNull(
        stargateEnvironmentInfo,
        "Stargate coordinator is not available in " + context.getUniqueId());

    ApiServiceParameters params = parameters(spec, context);

    if (service != null) {
      if (service.matches(stargateEnvironmentInfo, spec, params)) {
        LOG.info(
            "Reusing matching {} Service {} for {}",
            params.serviceName(),
            spec,
            context.getUniqueId());
        return Optional.empty();
      }

      LOG.info(
          "Closing old {} Service due to spec mismatch within {}",
          params.serviceName(),
          context.getUniqueId());
      service.close();
    }

    LOG.info(
        "Starting {} Service with spec {} for {}",
        params.serviceName(),
        spec,
        context.getUniqueId());

    ApiService svc = new ApiService(stargateEnvironmentInfo, spec, params);
    svc.start();
    return Optional.of(svc);
  }

  @Override
  public boolean supportsParameter(ParameterContext pc, ExtensionContext ec)
      throws ParameterResolutionException {
    return pc.getParameter().getType() == ApiServiceConnectionInfo.class;
  }

  @Override
  public Object resolveParameter(ParameterContext pc, ExtensionContext ec)
      throws ParameterResolutionException {
    return getResource(ec).orElseThrow(() -> new IllegalStateException("Cluster not available"));
  }

  protected static class ApiService extends ExternalResource.Holder
      implements ApiServiceConnectionInfo, AutoCloseable {

    private final StargateEnvironmentInfo stargateEnvironmentInfo;
    private final ApiServiceSpec spec;
    private final ApiServiceParameters parameters;
    private final Instance instance;

    private ApiService(
        StargateEnvironmentInfo stargateEnvironmentInfo,
        ApiServiceSpec spec,
        ApiServiceParameters parameters)
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
        ApiServiceSpec spec,
        ApiServiceParameters parameters) {
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
      return parameters.servicePort();
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

    private Instance(StargateEnvironmentInfo stargateEnvironmentInfo, ApiServiceParameters params)
        throws Exception {
      super(params.serviceName(), 1, 1);

      cmd = new CommandLine("java");

      // add configured connection properties
      cmd.addArgument("-D" + params.servicePortPropertyName() + "=" + params.servicePort());
      cmd.addArgument(
          "-D"
              + params.bridgeHostPropertyName()
              + "="
              + stargateEnvironmentInfo.nodes().get(0).seedAddress());

      // TODO: get these property values from stargateEnvironmentInfo
      cmd.addArgument("-D" + params.bridgePortPropertyName() + "=" + BRIDGE_PORT);
      cmd.addArgument("-D" + params.bridgeTokenPropertyName() + "=" + BRIDGE_TOKEN);

      for (Entry<String, String> e : params.systemProperties().entrySet()) {
        cmd.addArgument("-D" + e.getKey() + "=" + e.getValue());
      }

      if (isDebug()) {
        int debuggerPort = 5200;
        cmd.addArgument(
            "-agentlib:jdwp=transport=dt_socket,server=n,suspend=y,"
                + "address=localhost:"
                + debuggerPort);
      }

      cmd.addArgument("-jar");
      cmd.addArgument(getStarterJar(params).getAbsolutePath());

      addStdOutListener(
          (node, line) -> {
            if (line.contains(params.serviceStartedMessage())) {
              ready();
            }
          });
    }

    private static File getStarterJar(ApiServiceParameters params) {
      String dir = System.getProperty(params.serviceLibDirProperty());
      if (dir == null) {
        throw new IllegalStateException(
            params.serviceLibDirProperty() + " system property is not set.");
      }
      File libDir = new File(dir);
      File[] files = libDir.listFiles();
      Assertions.assertNotNull(files, "No files in " + libDir.getAbsolutePath());
      return Arrays.stream(files)
          .filter(f -> f.getName().startsWith(params.serviceJarBase()))
          .filter(f -> f.getName().endsWith(".jar"))
          .findFirst()
          .orElseThrow(
              () ->
                  new IllegalStateException(
                      "Unable to find "
                          + params.serviceJarBase()
                          + "*.jar in: "
                          + libDir.getAbsolutePath()));
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
