package io.stargate.it.proxy;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotates a test parameter to denote that proxy addresses should be passed to this parameter.
 *
 * <p>This is used when extending a test class with {@link ProxyExtension}. The parameter should use
 * the type {@code List<InetSocketAddress>}
 */
@Target(ElementType.PARAMETER)
@Retention(RetentionPolicy.RUNTIME)
public @interface ProxyAddresses {}
