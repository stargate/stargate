package io.stargate.sgv2.api.common.config;

import static java.lang.annotation.ElementType.*;

import io.stargate.bridge.proto.QueryOuterClass;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import javax.validation.Constraint;
import javax.validation.Payload;

/** Annotation to validate serial consistency */
@Target({METHOD, FIELD, ANNOTATION_TYPE, CONSTRUCTOR, PARAMETER, TYPE_USE})
@Retention(RetentionPolicy.RUNTIME)
@Constraint(validatedBy = SerialConsistencyValidator.class)
public @interface SerialConsistencyValid {
  QueryOuterClass.Consistency[] anyOf();

  String message() default "must be any of {anyOf}";

  Class<?>[] groups() default {};

  Class<? extends Payload>[] payload() default {};
}
