package io.stargate.it.proxy;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Annotates a test class or method to check whether DNS has been correctly setup.
 *
 * <p>A correct setup will have a proxy DNS name with an A record for each proxy. An invalid setup
 * will result in skipping the test.
 */
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith(ProxyDnsCondition.class)
public @interface SkipIfProxyDnsInvalid {}
