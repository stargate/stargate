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
import org.junit.platform.commons.support.SearchOption;

/**
 * A condition that implements the logic for {@link IfBundleAvailable}. It checks to make sure a
 * bundle exists so that tests can be run against it.
 */
public class BundleAvailableCondition implements ExecutionCondition {
  @Override
  public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
    IfBundleAvailable bundleSkip = context.getElement()
            .flatMap(element -> {
              if (element instanceof Class) {
                return AnnotationSupport.findAnnotation((Class<?>) element, IfBundleAvailable.class, SearchOption.INCLUDE_ENCLOSING_CLASSES);
              } else if (element instanceof Method) {
                return AnnotationSupport.findAnnotation(((Method) element).getDeclaringClass(), IfBundleAvailable.class, SearchOption.INCLUDE_ENCLOSING_CLASSES);
              } else {
                return Optional.empty();
              }
            })
            .orElseThrow(() -> new IllegalStateException("Can not locate the @IfBundleAvailable annotation on the annotated element."));

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
