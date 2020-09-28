package io.stargate.core;

import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BundleUtils {
  private static final Logger logger = LoggerFactory.getLogger(CoreActivator.class);

  /** Gets the instance of the newly registered service. */
  public static Object getRegisteredService(BundleContext context, ServiceEvent serviceEvent) {
    if (serviceEvent.getType() != ServiceEvent.REGISTERED) {
      return null;
    }

    String[] objectClass = (String[]) serviceEvent.getServiceReference().getProperty("objectClass");
    logger.info("Service of type " + objectClass[0] + " registered.");
    return context.getService(serviceEvent.getServiceReference());
  }
}
