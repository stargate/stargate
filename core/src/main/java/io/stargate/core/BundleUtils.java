package io.stargate.core;

import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceEvent;
import org.osgi.framework.ServiceReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BundleUtils {
  private static final Logger logger = LoggerFactory.getLogger(CoreActivator.class);

  /** Gets the instance of the newly registered service or null if the type does not match. */
  @SuppressWarnings("unchecked")
  public static <T> T getRegisteredService(
      BundleContext context, ServiceEvent serviceEvent, Class<T> serviceType) {
    if (serviceEvent.getType() != ServiceEvent.REGISTERED) {
      return null;
    }

    String[] objectClass = (String[]) serviceEvent.getServiceReference().getProperty("objectClass");
    logger.info("Service of type " + objectClass[0] + " registered.");

    ServiceReference<?> serviceReference = serviceEvent.getServiceReference();
    Object service = context.getService(serviceReference);
    if (!serviceType.isInstance(service)) {
      context.ungetService(serviceReference);
      return null;
    }
    return (T) service;
  }
}
