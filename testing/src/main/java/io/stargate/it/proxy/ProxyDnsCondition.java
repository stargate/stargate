package io.stargate.it.proxy;

import com.datastax.oss.driver.shaded.guava.common.net.InetAddresses;
import io.stargate.it.storage.StargateParameters;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A condition that verifies that DNS is setup correctly to contain the IP addresses for all the
 * proxies in the cluster.
 *
 * <p>This uses {@link ProxySpec#verifyProxyDnsName()} to resolve the IP addresses so make sure that
 * matches {@link StargateParameters#proxyDnsName()} if changed from the default value.
 */
public class ProxyDnsCondition implements ExecutionCondition {
  private static final Logger LOG = LoggerFactory.getLogger(ProxyDnsCondition.class);

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

    // Use the default spec if an explicit one is not on the class
    ProxySpec proxySpec = maybeProxySpec.orElse(ProxyExtension.DEFAULT_PROXY_SPEC);

    List<InetAddress> resolvedAddresses;
    try {
      resolvedAddresses =
          Arrays.stream(InetAddress.getAllByName(proxySpec.verifyProxyDnsName()))
              .collect(Collectors.toList());
    } catch (UnknownHostException e) {
      String msg =
          String.format(
              "Unable to determine addresses for proxy DNS: %s", proxySpec.verifyProxyDnsName());
      LOG.error(msg, e);
      return ConditionEvaluationResult.disabled(msg);
    }

    InetAddress proxyAddress;
    try {
      proxyAddress = InetAddress.getByName(proxySpec.startingLocalAddress());
    } catch (UnknownHostException e) {
      String msg =
          String.format("Invalid starting local address: %s", proxySpec.startingLocalAddress());
      LOG.error(msg, e);
      return ConditionEvaluationResult.disabled(msg);
    }
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
}
