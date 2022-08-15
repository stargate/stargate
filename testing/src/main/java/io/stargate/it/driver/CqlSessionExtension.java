package io.stargate.it.driver;

import static org.awaitility.Awaitility.await;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.datastax.oss.driver.internal.core.loadbalancing.DcInferringLoadBalancingPolicy;
import io.stargate.it.storage.StargateEnvironmentInfo;
import io.stargate.it.storage.StargateExtension;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import javax.validation.constraints.NotNull;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.platform.commons.support.AnnotationSupport;
import org.junit.platform.commons.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages a Java driver {@link CqlSession} in Junit 5 tests.
 *
 * <h3>Requirements</h3>
 *
 * This extension requires {@link StargateExtension} to be activated as well, and the Stargate
 * node(s) must be started first. This can generally be achieved by declaring the annotations in the
 * right order. The created session will use all the Stargate nodes as contact points.
 *
 * <h3>Lifecycle</h3>
 *
 * This extension can be declared either at the class level, or at the method level.
 *
 * <h4>Class level</h4>
 *
 * <pre>
 * &#64;UseStargateCoordinator
 * &#64;ExtendWith(CqlSessionExtension.class)
 * public class SomeTestClass {
 *   &#64;Test
 *   public void test1(CqlSession session) {}
 *   &#64;Test
 *   public void test2(CqlSession session) {}
 * }
 * </pre>
 *
 * In this mode, the extension creates a single session for all test methods. In other words, {@code
 * session} in {@code test1} and {@code test2} refer to the same instance. The session is shut down
 * once all methods have executed.
 *
 * <h4>Method level</h4>
 *
 * <pre>
 * &#64;UseStargateCoordinator
 * public class SomeTestClass {
 *   &#64;Test
 *   &#64;ExtendWith(CqlSessionExtension.class)
 *   public void test1(CqlSession session) {}
 *   &#64;Test
 *   &#64;ExtendWith(CqlSessionExtension.class)
 *   public void test2(CqlSession session) {}
 * }
 * </pre>
 *
 * In this mode, the extension creates a separate session before every method, and shuts it down
 * immediately after. In other words, {@code session} in {@code test1} and {@code test2} refer to
 * different instances.
 *
 * <p>It's possible to declare the extension on some methods but not others; sessions will be
 * created or not, accordingly.
 *
 * <p>If the extension is declared both on the class level and on some methods, the method-level
 * annotations are ignored. If you need method-level sessions in addition to the global one, it is
 * possible to do it manually by injecting a builder (see the "Parameter injection" section below).
 *
 * <h3>Keyspace creation</h3>
 *
 * By default, the extension creates a dedicated keyspace for the lifecycle of each created session,
 * and drops it immediately after the test class/method has executed. Keyspace names are generated
 * in a way that guarantees uniqueness, to avoid conflicts between tests.
 *
 * <p>A {@code USE ks} statement is automatically issued, so that the session is already "connected"
 * to the keyspace when your test code starts executing. In general you don't need to prefix table
 * names in your queries, and tests are often oblivious to the actual keyspace name. However it can
 * be injected if you need it (see the "Parameter injection" section below).
 *
 * <h3>Customization</h3>
 *
 * Certain aspects of the session creation can be controlled with the {@link CqlSessionSpec}
 * annotation. See the javadocs of that type for more details.
 *
 * <h3>Parameter injection</h3>
 *
 * The extension will detect and inject the following parameters on JUnit test methods
 * (constructors, BeforeXxx or test methods, etc):
 *
 * <ul>
 *   <li>any parameter of type {@link CqlSession}: injected with the session that was created by the
 *       extension.
 *   <li>any parameter of type {@link CqlSessionBuilder}: injected with a new session builder
 *       pre-configured to connect to the Stargate cluster. This allows you to create new sessions
 *       with a different configuration in your test methods. Note that such sessions are not
 *       managed by the extension: <b>you must close them explicitly once you are done</b>, to avoid
 *       resource leaks. A try-with-resources block is generally the best way to do that. Also, you
 *       might want to use {@link CqlSessionHelper#waitForStargateNodes} if there are multiple
 *       Stargate nodes.
 *   <li>a parameter of type {@link CqlIdentifier} <b>AND annotated with {@link TestKeyspace}</b>:
 *       injected with the identifier of the keyspace that was created by the extension. This is
 *       useful if you need to look up schema metadata, use the keyspace from another session or
 *       API, etc.
 *   <li>any parameter of type {@link CqlSessionHelper}: returns an instance of that interface, that
 *       contains utility methods (see its javadocs).
 * </ul>
 *
 * <h3>Parallel execution</h3>
 *
 * JUnit 5 allows parallel execution of test classes, or even parallel execution of test methods
 * within the same class. This extension is thread-safe and will work in either scenario. Note
 * however that using a class-level extension with parallel methods can be tricky: the methods will
 * be accessing the same keyspace concurrently, which can have unintended side effects if they
 * mutate the same tables. It's probably a good idea to keep parallelism at the class level.
 */
public class CqlSessionExtension
    implements BeforeAllCallback,
        BeforeEachCallback,
        AfterEachCallback,
        AfterAllCallback,
        ParameterResolver {

  private static final Logger LOG = LoggerFactory.getLogger(CqlSessionExtension.class);
  private static final int KEYSPACE_NAME_MAX_LENGTH = 48;

  // Tracks whether the extension was configured per-class or per-method.
  private volatile Lifecycle sessionLifecycle;

  private volatile StargateEnvironmentInfo stargate;
  private volatile CqlSessionSpec cqlSessionSpec;
  private volatile CqlIdentifier keyspaceId;
  private volatile CqlSession session;
  private volatile List<InetSocketAddress> contactPoints;

  @Override
  public void beforeAll(ExtensionContext context) {
    LOG.debug("Starting per-class execution for {}", context.getElement());
    // beforeAll and afterAll are never called if we're per-method, that's how we figure out the
    // lifecycle
    sessionLifecycle = Lifecycle.PER_CLASS;
    createSession(context);
  }

  @Override
  public void beforeEach(ExtensionContext context) {
    if (sessionLifecycle == null) {
      LOG.debug("Starting per-method execution for {}", context.getElement());
      sessionLifecycle = Lifecycle.PER_METHOD;
      createSession(context);
    }
  }

  @Override
  public void afterEach(ExtensionContext context) {
    assert sessionLifecycle != null;
    if (sessionLifecycle == Lifecycle.PER_METHOD) {
      LOG.debug("Stopping per-method execution for {}", context.getElement());
      destroySession(context);
    }
  }

  @Override
  public void afterAll(ExtensionContext context) {
    LOG.debug("Stopping per-class execution for {}", context.getElement());
    destroySession(context);
  }

  @Override
  public boolean supportsParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    Parameter parameter = parameterContext.getParameter();
    Class<?> type = parameter.getType();
    return type == CqlSession.class
        || type == CqlSessionBuilder.class
        || type == CqlSessionHelper.class
        || (type == CqlIdentifier.class && parameter.getAnnotation(TestKeyspace.class) != null);
  }

  @Override
  public Object resolveParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    Class<?> type = parameterContext.getParameter().getType();
    if (type == CqlSession.class) {
      if (session == null) {
        throw new IllegalStateException(
            String.format(
                "Can't inject a session because @%s.createSession() == false",
                CqlSessionSpec.class.getSimpleName()));
      } else {
        return session;
      }
    } else if (type == CqlSessionBuilder.class) {
      return newSessionBuilder(extensionContext);
    } else if (type == CqlSessionHelper.class) {
      return (CqlSessionHelper) this::waitForStargateNodes;
    } else if (type == CqlIdentifier.class) {
      if (keyspaceId == null) {
        throw new IllegalStateException(
            String.format(
                "Can't inject the keyspace id because @%s.createKeyspace == false",
                CqlSessionSpec.class.getSimpleName()));
      } else {
        return keyspaceId;
      }
    } else {
      throw new AssertionError("Unsupported parameter");
    }
  }

  private void createSession(ExtensionContext context) {
    stargate =
        (StargateEnvironmentInfo)
            context.getStore(ExtensionContext.Namespace.GLOBAL).get(StargateExtension.STORE_KEY);
    if (stargate == null) {
      throw new IllegalStateException(
          String.format(
              "%s can only be used in conjunction with %s (make sure it is declared last)",
              CqlSessionExtension.class.getSimpleName(), StargateExtension.class.getSimpleName()));
    }

    cqlSessionSpec = getCqlSessionSpec(context);

    try {
      contactPoints =
          cqlSessionSpec.contactPointResolver().getConstructor().newInstance().resolve(context);
    } catch (Exception e) {
      throw new IllegalStateException(
          String.format(
              "Unable to resolve contact points using @%s.contactPointResolver == %s.class",
              CqlSessionSpec.class.getSimpleName(),
              cqlSessionSpec.contactPointResolver().getSimpleName()),
          e);
    }

    if (cqlSessionSpec.createSession()) {
      LOG.debug("Creating new session for {}", context.getElement());
      session = newSessionBuilder(context).build();
      waitForStargateNodes(session);

      if (cqlSessionSpec.createKeyspace()) {
        keyspaceId = generateKeyspaceId(context);
        LOG.debug("Creating keyspace {}", keyspaceId.asCql(true));

        session.execute(
            String.format(
                "CREATE KEYSPACE IF NOT EXISTS %s "
                    + "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
                keyspaceId.asCql(false)));
        session.execute(String.format("USE %s", keyspaceId.asCql(false)));
      }

      for (String query : cqlSessionSpec.initQueries()) {
        session.execute(query);
      }
    } else {
      LOG.debug(
          "Not creating session for {} because @{}.createSession() == false",
          context.getElement(),
          CqlSessionSpec.class.getSimpleName());
    }
  }

  private void destroySession(ExtensionContext context) {
    if (session != null) {
      try {
        if (cqlSessionSpec.dropKeyspace()) {
          LOG.debug("Dropping keyspace {}", keyspaceId.asCql(true));
          session.execute(String.format("DROP KEYSPACE IF EXISTS %s", keyspaceId.asCql(false)));
        }
      } finally {
        LOG.debug("Destroying session for {}", context.getElement());
        session.close();
      }
    }
  }

  private CqlSessionSpec getCqlSessionSpec(ExtensionContext context) {
    AnnotatedElement element =
        context
            .getElement()
            .orElseThrow(() -> new IllegalStateException("Expected to have an element"));
    Optional<CqlSessionSpec> maybeSpec =
        AnnotationSupport.findAnnotation(element, CqlSessionSpec.class);
    if (!maybeSpec.isPresent() && element instanceof Method) {
      maybeSpec =
          AnnotationSupport.findAnnotation(
              ((Method) element).getDeclaringClass(), CqlSessionSpec.class);
    }
    return maybeSpec.orElse(DefaultCqlSessionSpec.INSTANCE);
  }

  private CqlSessionBuilder newSessionBuilder(ExtensionContext context) {
    OptionsMap options = defaultConfig();
    maybeCustomizeOptions(options, cqlSessionSpec, context);
    CqlSessionBuilder builder = CqlSession.builder().withAuthCredentials("cassandra", "cassandra");
    builder = maybeCustomizeBuilder(builder, cqlSessionSpec, context);
    builder.withConfigLoader(DriverConfigLoader.fromMap(options)).addContactPoints(contactPoints);
    return builder;
  }

  /** @see CqlSessionHelper#waitForStargateNodes(CqlSession) */
  private void waitForStargateNodes(CqlSession session) {
    int expectedNodeCount = contactPoints.size();
    if (expectedNodeCount > 1) {
      await()
          .atMost(Duration.ofMinutes(10))
          .pollInterval(Duration.ofSeconds(10))
          .until(
              () -> {
                boolean connected = session.getMetadata().getNodes().size() == expectedNodeCount;
                LOG.debug(
                    "Expected: {}, in driver metadata: {}, in system tables: {}",
                    expectedNodeCount,
                    session.getMetadata().getNodes().size(),
                    session.execute("SELECT * FROM system.peers").all().size() + 1);

                return connected;
              });
    }
  }

  private void maybeCustomizeOptions(
      OptionsMap options, CqlSessionSpec cqlSessionSpec, ExtensionContext context) {
    String methodName = cqlSessionSpec.customOptions().trim();
    if (!methodName.isEmpty()) {
      Method method =
          ReflectionUtils.getRequiredMethod(
              context.getRequiredTestClass(), methodName, OptionsMap.class);
      Object testInstance = context.getTestInstance().orElse(null);
      ReflectionUtils.invokeMethod(method, testInstance, options);
    }
  }

  private CqlSessionBuilder maybeCustomizeBuilder(
      CqlSessionBuilder builder, CqlSessionSpec cqlSessionSpec, ExtensionContext context) {
    String methodName = cqlSessionSpec.customBuilder().trim();
    if (methodName.isEmpty()) {
      return builder;
    } else {
      Method method =
          ReflectionUtils.getRequiredMethod(
              context.getRequiredTestClass(), methodName, CqlSessionBuilder.class);
      if (method.getReturnType() != CqlSessionBuilder.class) {
        throw new IllegalArgumentException(
            String.format(
                "The method referenced by %s.customBuilder() must return %s",
                CqlSessionSpec.class.getSimpleName(), CqlSessionBuilder.class.getSimpleName()));
      }
      Object testInstance = context.getTestInstance().orElse(null);
      return (CqlSessionBuilder) ReflectionUtils.invokeMethod(method, testInstance, builder);
    }
  }

  @NotNull
  private OptionsMap defaultConfig() {
    OptionsMap config;
    config = OptionsMap.driverDefaults();
    config.put(
        TypedDriverOption.LOAD_BALANCING_POLICY_CLASS,
        DcInferringLoadBalancingPolicy.class.getName());
    config.put(TypedDriverOption.REQUEST_TIMEOUT, Duration.ofMinutes(3));
    config.put(TypedDriverOption.METADATA_SCHEMA_REQUEST_TIMEOUT, Duration.ofMinutes(3));
    config.put(TypedDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, Duration.ofMinutes(3));
    config.put(TypedDriverOption.CONTROL_CONNECTION_TIMEOUT, Duration.ofMinutes(3));
    config.put(TypedDriverOption.HEARTBEAT_TIMEOUT, Duration.ofMinutes(1));
    config.put(TypedDriverOption.HEARTBEAT_INTERVAL, Duration.ofMinutes(1));
    config.put(TypedDriverOption.REQUEST_TRACE_INTERVAL, Duration.ofSeconds(5));
    config.put(TypedDriverOption.REQUEST_TRACE_ATTEMPTS, 180 / 5);
    config.put(TypedDriverOption.REQUEST_WARN_IF_SET_KEYSPACE, false);
    return config;
  }

  private static CqlIdentifier generateKeyspaceId(ExtensionContext context) {
    String targetName =
        context
            .getTestMethod()
            .map(Method::getName)
            .orElseGet(
                () ->
                    context
                        .getTestClass()
                        .map(Class::getSimpleName)
                        .orElseThrow(
                            () ->
                                new IllegalStateException(
                                    "Could not infer a class or method name")));
    String keyspaceName = "ks_" + new Date().getTime() + "_" + targetName;
    if (keyspaceName.length() > KEYSPACE_NAME_MAX_LENGTH) {
      keyspaceName = keyspaceName.substring(0, KEYSPACE_NAME_MAX_LENGTH);
    }
    return CqlIdentifier.fromInternal(keyspaceName);
  }

  @CqlSessionSpec
  private static class DefaultCqlSessionSpec {
    private static final CqlSessionSpec INSTANCE =
        DefaultCqlSessionSpec.class.getAnnotation(CqlSessionSpec.class);
  }
}
