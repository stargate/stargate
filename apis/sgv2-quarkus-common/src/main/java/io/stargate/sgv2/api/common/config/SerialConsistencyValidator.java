package io.stargate.sgv2.api.common.config;

import io.stargate.bridge.proto.QueryOuterClass;
import java.util.Arrays;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;

/** Validator to check if the consistency provided is valid serial consistency */
public class SerialConsistencyValidator
    implements ConstraintValidator<SerialConsistencyValid, QueryOuterClass.Consistency> {
  private QueryOuterClass.Consistency[] validConsistencies;

  @Override
  public void initialize(SerialConsistencyValid constraint) {
    this.validConsistencies = constraint.anyOf();
  }

  @Override
  public boolean isValid(QueryOuterClass.Consistency value, ConstraintValidatorContext context) {
    return value != null && Arrays.binarySearch(validConsistencies, value) >= 0;
  }
}
