package io.stargate.it.storage;

import com.datastax.oss.driver.api.testinfra.ccm.CcmBridge;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

public class IsDseCondition implements ExecutionCondition {
  @Override
  public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext extensionContext) {
    if (CcmBridge.DSE_ENABLEMENT) {
      return ConditionEvaluationResult.enabled("Using DSE persistence");
    } else {
      return ConditionEvaluationResult.disabled("Not using DSE persistence");
    }
  }
}
