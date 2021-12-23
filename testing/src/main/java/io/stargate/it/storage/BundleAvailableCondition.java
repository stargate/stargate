package io.stargate.it.storage;

import java.io.File;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Optional;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.commons.support.AnnotationSupport;

/**
 * A condition that implements the logic for {@link IfBundleAvailable}. It checks to make sure a
 * bundle exists so that tests can be run against it.
 */
public class BundleAvailableCondition implements ExecutionCondition {
  @Override
  public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
    AnnotatedElement element =
        context
            .getElement()
            .orElseThrow(() -> new IllegalStateException("Expected to have an element"));
    Optional<IfBundleAvailable> maybeBundleSkip =
        AnnotationSupport.findAnnotation(element, IfBundleAvailable.class);
    if (!maybeBundleSkip.isPresent() && element instanceof Method) {
      maybeBundleSkip =
          AnnotationSupport.findAnnotation(
              ((Method) element).getDeclaringClass(), IfBundleAvailable.class);
    }

    IfBundleAvailable bundleSkip =
        maybeBundleSkip.orElseThrow(
            () -> new IllegalStateException("IfBundleAvailable annotation not present"));

    String bundleName = bundleSkip.bundleName();
    File[] files = StargateExtension.LIB_DIR.listFiles();
    return Arrays.stream(files)
        .filter(f -> f.getName().startsWith(bundleName))
        .filter(f -> f.getName().endsWith(".jar"))
        .findFirst()
        .map(f -> ConditionEvaluationResult.enabled(String.format("Found bundle '%s'", bundleName)))
        .orElse(
            ConditionEvaluationResult.disabled(
                String.format("Unable to find bundle '%s'", bundleName)));
  }
}
