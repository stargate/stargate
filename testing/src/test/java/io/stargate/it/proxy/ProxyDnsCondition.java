package io.stargate.it.proxy;

import com.google.common.net.InetAddresses;
import java.io.UncheckedIOException;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.commons.support.AnnotationSupport;

public class ProxyDnsCondition implements ExecutionCondition {
  @Override
  public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
    AnnotatedElement element =
        context
            .getElement()
            .orElseThrow(() -> new IllegalStateException("Expected to have an element"));
    Optional<ProxySpec> maybeProxySpec = AnnotationSupport.findAnnotation(element, ProxySpec.class);
    if (!maybeProxySpec.isPresent() && element instanceof Method) {
      maybeProxySpec =
          AnnotationSupport.findAnnotation(((Method) element).getDeclaringClass(), ProxySpec.class);
    }

    ProxySpec proxySpec = maybeProxySpec.orElse(ProxyExtension.DEFAULT_PROXY_SPEC);

    List<InetAddress> resolvedAddresses;
    try {
      resolvedAddresses =
          Arrays.stream(InetAddress.getAllByName(proxySpec.verifyProxyDnsName()))
              .map(a -> getByName(a.getHostAddress()))
              .collect(Collectors.toList());
    } catch (UnknownHostException e) {
      throw new UncheckedIOException("Unable to determine addresses for proxy DNS", e);
    }

    InetAddress proxyAddress = getByName(proxySpec.startingLocalAddress());
    for (int i = 0; i < proxySpec.numProxies(); ++i) {
      if (!resolvedAddresses.contains(proxyAddress)) {
        return ConditionEvaluationResult.disabled(
            String.format(
                "Proxy DNS setup incorrectly, '%s' should contain the address '%s'",
                proxySpec.verifyProxyDnsName(), proxyAddress.getHostAddress()));
      }
      proxyAddress = InetAddresses.increment(proxyAddress);
    }
    return ConditionEvaluationResult.enabled("Proxy DNS setup correctly");
  }

  InetAddress getByName(String address) {
    try {
      return InetAddress.getByName(address);
    } catch (UnknownHostException e) {
      throw new UncheckedIOException("Invalid address string", e);
    }
  }
}
