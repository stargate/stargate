/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.core.activator;

import io.stargate.core.BundleUtils;
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

  private final List<Class<?>> dependentServices;

  private BundleContext context;

  private LinkedHashMap<Class<?>, Object> registeredServices;

  private Class<?> targetServiceClass;

  public boolean started;

  public BaseActivator(
      String activatorName, List<Class<?>> dependentServices, Class<?> targetServiceClass) {
    this.activatorName = activatorName;
    this.dependentServices = dependentServices;
    this.registeredServices = createMapWithNullValues(dependentServices);
    this.targetServiceClass = targetServiceClass;
  }

  private LinkedHashMap<Class<?>, Object> createMapWithNullValues(
      List<Class<?>> dependentServices) {
    LinkedHashMap<Class<?>, Object> map = new LinkedHashMap<>(dependentServices.size());
    for (Class<?> dependentService : dependentServices) {
      map.put(dependentService, null);
    }
    return map;
  }

  @Override
  public synchronized void start(BundleContext context) throws InvalidSyntaxException {
    logger.info("Starting {} ...", activatorName);
    this.context = context;

    List<ServiceReference<?>> serviceReferences = new ArrayList<>();
    for (Class<?> dependentService : dependentServices) {
      ServiceReference<?> serviceReference =
          context.getServiceReference(dependentService.getName());
      if (serviceReference == null) {
        logger.debug(
            "{} service is null, registering a listener to get notification when it will be ready.",
            dependentService.getName());
        context.addServiceListener(
            this, String.format("(objectClass=%s)", dependentService.getName()));
      }
      serviceReferences.add(serviceReference);
    }

    if (serviceReferences.stream().anyMatch(Objects::isNull)) {
      // It will be started once all dependent services are registered
      return;
    }

    List<Object> startedServices = new ArrayList<>(serviceReferences.size());
    for (ServiceReference<?> serviceReference : serviceReferences) {
      startedServices.add(context.getService(serviceReference));
    }

    startServiceInternal(startedServices);
  }

  @Override
  public synchronized void stop(BundleContext context) throws Exception {}

  @Override
  public void serviceChanged(ServiceEvent event) {
    for (Class<?> dependentService : dependentServices) {
      Object registeredService = BundleUtils.getRegisteredService(context, event, dependentService);
      if (registeredService != null) {
        // capture registration only if the instance is not null
        registeredServices.put(dependentService, registeredService);
      }
    }
    if (registeredServices.values().stream().allMatch(Objects::nonNull)) {
      startServiceInternal(new LinkedList<>(registeredServices.values()));
    }
  }

  private void startServiceInternal(List<Object> dependentServices) {
    if (started) {
      logger.info("The {} is already started. Ignoring the start request.", activatorName);
      return;
    }
    started = true;
    ServiceAndProperties service = createService(dependentServices);
    context.registerService(targetServiceClass.getName(), service.service, service.properties);
    logger.info("Started {}....", activatorName);
  }

  protected abstract ServiceAndProperties createService(List<Object> dependentServices);

  public static class ServiceAndProperties {
    private final Object service;

    private final Hashtable<String, String> properties;

    public ServiceAndProperties(Object service, Hashtable<String, String> properties) {
      this.service = service;
      this.properties = properties;
    }
  }
}
