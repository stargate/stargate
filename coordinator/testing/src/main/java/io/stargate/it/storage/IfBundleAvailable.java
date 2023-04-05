package io.stargate.it.storage;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Annotates a test class or method to check whether a bundle has been built and deployed i.e. it's
 * available for testing.
 *
 * <p>Uses {@link BundleAvailableCondition}.
 */
@Inherited
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith(BundleAvailableCondition.class)
public @interface IfBundleAvailable {

  /** The name of the bundle to check for. */
  String bundleName();
}
