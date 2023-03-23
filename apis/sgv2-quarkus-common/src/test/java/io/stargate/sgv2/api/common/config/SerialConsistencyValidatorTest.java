package io.stargate.sgv2.api.common.config;

import static org.assertj.core.api.Assertions.assertThat;

import io.stargate.bridge.proto.QueryOuterClass;
import java.util.Set;
import java.util.function.Predicate;
import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import org.junit.jupiter.api.Test;

public class SerialConsistencyValidatorTest {

  private record TestRecord(
      @SerialConsistencyValid(
              anyOf = {
                QueryOuterClass.Consistency.SERIAL,
                QueryOuterClass.Consistency.LOCAL_SERIAL
              })
          QueryOuterClass.Consistency serialConsistency) {}
  ;

  @Test
  public void validSerialConsistency() {
    ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
    Validator validator = factory.getValidator();

    Set<ConstraintViolation<TestRecord>> constraintViolations =
        validator.validate(new TestRecord(QueryOuterClass.Consistency.SERIAL));
    assertThat(constraintViolations.size()).isEqualTo(0);

    constraintViolations =
        validator.validate(new TestRecord(QueryOuterClass.Consistency.LOCAL_SERIAL));
    assertThat(constraintViolations.size()).isEqualTo(0);
  }

  @Test
  public void invalidSerialConsistency() {
    ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
    Validator validator = factory.getValidator();

    Set<ConstraintViolation<TestRecord>> constraintViolations =
        validator.validate(new TestRecord(QueryOuterClass.Consistency.LOCAL_ONE));
    assertThat(constraintViolations.size()).isEqualTo(1);

    assertThat(constraintViolations)
        .anyMatch(
            forVariable("serialConsistency")
                .and(havingMessage("must be any of [SERIAL, LOCAL_SERIAL]")));

    constraintViolations = validator.validate(new TestRecord(null));
    assertThat(constraintViolations.size()).isEqualTo(1);

    assertThat(constraintViolations)
        .anyMatch(
            forVariable("serialConsistency")
                .and(havingMessage("must be any of [SERIAL, LOCAL_SERIAL]")));
  }

  public static Predicate<ConstraintViolation<TestRecord>> forVariable(String propertyPath) {
    return l -> propertyPath.equals(l.getPropertyPath().toString());
  }

  public static Predicate<ConstraintViolation<TestRecord>> havingMessage(String message) {
    return l -> message.equals(l.getMessage().toString());
  }
}
