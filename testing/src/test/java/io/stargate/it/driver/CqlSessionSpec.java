package io.stargate.it.driver;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import io.stargate.it.storage.StargateEnvironmentInfo;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotates a Junit 5 class or test method to customize certain aspects of the driver session
 * created by {@link CqlSessionExtension}.
 *
 * <p>If the extension is configured per class, this annotation must be at the class level. If the
 * extension is configured per method, this annotation can either be at the method level, or at the
 * class level (from where it will be inherited by methods which don't re-declare it).
 */
@Target({ElementType.TYPE, ElementType.METHOD})
@Inherited
@Retention(RetentionPolicy.RUNTIME)
public @interface CqlSessionSpec {

  /**
   * Whether to create a temporary keyspace for the current test.
   *
   * <p>The extension will execute a {@code USE ...} statement, so the session is already connected
   * to the keyspace when the test starts.
   *
   * <p>If your test needs the name of the keyspace, add a {@link CqlIdentifier} parameter to a
   * BeforeXxx or test method, and annotate it with {@link TestKeyspace}. It will get injected by
   * the extension.
   */
  boolean createKeyspace() default true;

  /**
   * Whether to drop the temporary keyspace after the current test.
   *
   * <p>This has no effect if {@link #createKeyspace()} wasn't set.
   */
  boolean dropKeyspace() default true;

  /**
   * The name of a method that will be invoked to customize driver options before creating the
   * session.
   *
   * <p>The method must be void and take a single {@link OptionsMap} parameter. It must be static if
   * the extension is configured per class.
   *
   * <p>The options are already initialized to some defaults when the method gets called, but it is
   * free to change any of them, create new profiles, etc.
   */
  String customOptions() default "";

  /**
   * The name of a method that will be invoked to customize the session builder before creating the
   * session.
   *
   * <p>The method must take a single {@link CqlSessionBuilder} parameter and return a {@link
   * CqlSessionBuilder}. It must be static if the extension is configured per class.
   *
   * <p>If you need to customize driver options, use {@link #customOptions()} instead of this
   * method.
   */
  String customBuilder() default "";

  /**
   * A list of CQL queries that will be executed before the current test.
   *
   * <p>If {@link #createKeyspace()} was set, the session will already be connected to the temporary
   * keyspace when these queries get executed. Otherwise, the session will not be connected to any
   * keyspace, and any schema elements must be referenced by their qualified name.
   */
  String[] initQueries() default {};

  /**
   * Whether to implicitly create a session.
   *
   * <p>Unset this if you only inject {@link CqlSessionBuilder} parameters in your tests, and don't
   * need an implicitly created session.
   *
   * <p>If this is set to false, {@link #createKeyspace()}, {@link #dropKeyspace()} and {@link
   * #initQueries()} are ignored.
   */
  boolean createSession() default true;

  /**
   * The class to use for resolving contact points.
   *
   * <p>This can be used to customize the contact points used connect the session. The default
   * implementation resolves contact points using {@link StargateEnvironmentInfo#nodes()}.
   */
  Class<? extends ContactPointResolver> contactPointResolver() default
      DefaultContactPointResolver.class;
}
