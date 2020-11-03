package io.stargate.it.storage;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Annotates a test class or method to check whether the current running persistence backend is
 * using DSE.
 *
 * <p>The test/suite is skipped if the persistence backend is not running DSE.
 *
 * @see IsDseCondition
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD})
@ExtendWith(IsDseCondition.class)
public @interface SkipWhenNotDse {}
