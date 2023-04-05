package io.stargate.it.driver;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotates a method parameter in a Junit 5 test, to indicate that {@link CqlSessionExtension} must
 * inject it with the identifier of the temporary keyspace that it has created for the current test.
 *
 * <p>The parameter must have the type {@link CqlIdentifier}. The test must have been configured
 * with {@link CqlSessionSpec#createKeyspace()} == true, otherwise a runtime error will be thrown.
 */
@Target(ElementType.PARAMETER)
@Retention(RetentionPolicy.RUNTIME)
public @interface TestKeyspace {}
