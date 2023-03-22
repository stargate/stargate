package io.stargate.sgv2.api.common.config;

import io.stargate.bridge.proto.QueryOuterClass;
import java.util.Arrays;
import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

public class SerialConsistencyValidator
    implements ConstraintValidator<SerialConsistencyTypeAnnotation, QueryOuterClass.Consistency> {
  private QueryOuterClass.Consistency[] subset;

  @Override
  public void initialize(SerialConsistencyTypeAnnotation constraint) {
    this.subset = constraint.anyOf();
  }

  @Override
  public boolean isValid(QueryOuterClass.Consistency value, ConstraintValidatorContext context) {
    return value == null || Arrays.asList(subset).contains(value);
  }
}
