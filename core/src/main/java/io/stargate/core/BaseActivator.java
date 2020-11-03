package io.stargate.core;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceEvent;
import org.osgi.framework.ServiceListener;
import org.osgi.framework.ServiceReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseActivator implements BundleActivator, ServiceListener {
  private static final Logger logger = LoggerFactory.getLogger(BaseActivator.class);

  private final String activatorName;

  private final List<Class<?>> dependantServices;

  private BundleContext context;

  private LinkedHashMap<Class<?>, Object> registeredServices;

  private Class<?> targetServiceClass;

  public boolean started;

  public BaseActivator(
      String activatorName, List<Class<?>> dependantServices, Class<?> targetServiceClass) {
    this.activatorName = activatorName;
    this.dependantServices = dependantServices;
    this.registeredServices = createMapWithNullValues(dependantServices);
    this.targetServiceClass = targetServiceClass;
  }

  private LinkedHashMap<Class<?>, Object> createMapWithNullValues(
      List<Class<?>> dependantServices) {
    LinkedHashMap<Class<?>, Object> map = new LinkedHashMap<>(dependantServices.size());
    for (Class<?> dependantService : dependantServices) {
      map.put(dependantService, null);
    }
    return map;
  }

  @Override
  public synchronized void start(BundleContext context) throws InvalidSyntaxException {
    logger.info("Starting {} ...", activatorName);
    this.context = context;

    List<ServiceReference<?>> serviceReferences = new ArrayList<>();
    for (Class<?> dependantService : dependantServices) {
      ServiceReference<?> serviceReference =
          context.getServiceReference(dependantService.getName());
      if (serviceReference == null) {
        logger.debug(
            "{} service is null, registering a listener to get notification when it will be ready.",
            dependantService.getName());
        context.addServiceListener(
            this, String.format("(objectClass=%s)", dependantService.getName()));
      }
      serviceReferences.add(serviceReference);
    }

    if (serviceReferences.stream().anyMatch(Objects::isNull)) {
      // It will be started once all dependant services are registered
      return;
    }

    List<Object> startedServices = new ArrayList<>(serviceReferences.size());
    for (ServiceReference<?> serviceReference : serviceReferences) {
      startedServices.add(context.getService(serviceReference));
      startServiceInternal(startedServices);
    }
  }

  @Override
  public synchronized void stop(BundleContext context) throws Exception {}

  @Override
  public void serviceChanged(ServiceEvent event) {
    for (Class<?> dependantService : dependantServices) {
      Object registeredService = BundleUtils.getRegisteredService(context, event, dependantService);
      if (registeredService != null) {
        // capture registration only if the instance is not null
        registeredServices.put(dependantService, registeredService);
      }
    }
    if (registeredServices.values().stream().allMatch(Objects::nonNull)) {
      startServiceInternal(new LinkedList<>(registeredServices.values()));
    }
  }

  private void startServiceInternal(List<Object> dependantServices) {
    if (started) {
      logger.info("The {} is already started. Ignoring the start request.", activatorName);
      return;
    }
    started = true;
    ServiceAndProperties service = createService(dependantServices);
    context.registerService(targetServiceClass.getName(), service.service, service.properties);
    logger.info("Started {}....", activatorName);
  }

  protected abstract ServiceAndProperties createService(List<Object> dependantServices);

  public static class ServiceAndProperties {
    private final Object service;

    private final Hashtable<String, String> properties;

    public ServiceAndProperties(Object service, Hashtable<String, String> properties) {
      this.service = service;
      this.properties = properties;
    }
  }
}
