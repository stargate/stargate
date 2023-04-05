package io.stargate.it.proxy;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotates a test class or method that's been extended using {@link ProxyExtension}. It's useful
 * for modifying the functionality provided by that extension.
 */
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface ProxySpec {
  /**
   * The starting address used to bind proxy servers. If {@link #numProxies()} is {@code > 1} then
   * subsequent addresses will follow directly after this address e.g. "127.0.1.12", "127.0.1.13",
   * ...
   */
  String startingLocalAddress() default "127.0.1.11";

  /** The port to bind the proxy servers to. */
  int localPort() default 9043;

  /** The number of proxies to start. */
  int numProxies() default 1;

  /**
   * The DNS name used to contact the proxies. This is used by {@link ProxyDnsCondition} to verify
   * that DNS has been setup correctly.
   *
   * @see SkipIfProxyDnsInvalid
   */
  String verifyProxyDnsName() default "stargate.local";

  /**
   * Indicates whether proxy instances started from this specification can be shared among different
   * tests.
   */
  boolean shared() default false;
}
