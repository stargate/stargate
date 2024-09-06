package io.stargate.it.storage;

import com.datastax.oss.driver.api.testinfra.ccm.CcmBridge;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * A condition that checks if the persistence backend uses DSE.
 *
 * <p>The test/suite is skipped if the persistence backend is running DSE.
 */
public class IsNotDseCondition implements ExecutionCondition {
  @Override
  public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext extensionContext) {
    if (CcmBridge.DSE_ENABLEMENT) {
      return ConditionEvaluationResult.disabled("Using DSE persistence");
    } else {
      return ConditionEvaluationResult.enabled("Not using DSE persistence");
    }
  }
}
