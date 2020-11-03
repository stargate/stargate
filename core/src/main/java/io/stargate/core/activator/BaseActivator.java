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
import java.util.Dictionary;
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

  /**
   * @param activatorName - The name used when logging the progress of registration.
   * @param dependentServices - List of dependent services that needs to be retrieved using service
   *     reference.
   * @param targetServiceClass - This class will be used when registering the service.
   */
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

  /**
   * When the OSGi invokes the start() method, it will try to get all dependentServices passed to
   * this class's constructor. It will use the {@link BundleContext#getServiceReference(String)}. If
   * any of those services are not present, it will register the listeners for them using {@link
   * BundleContext#addServiceListener(ServiceListener, String)} and not start the target service
   * (see targetServiceClass). It will wait for a notification denoting that service was registered
   * using {@link this#serviceChanged(ServiceEvent)}. If all services are present, it will call the
   * user-provided {@link this#createService(List)} and register it in the OSGi using {@link
   * BundleContext#registerService(Class, Object, Dictionary)}.
   */
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
      } else {
        // if the service is present, get it
        registeredServices.put(dependentService, context.getService(serviceReference));
      }
      serviceReferences.add(serviceReference);
    }

    if (serviceReferences.stream().anyMatch(Objects::isNull)) {
      // It will be started once all dependent services are registered
      return;
    }

    // all dependant services are present
    startServiceInternal(new LinkedList<>(registeredServices.values()));
  }

  @Override
  public synchronized void stop(BundleContext context) throws Exception {}

  /**
   * It will try to match all dependentServices with a ServiceEvent notification. If the
   * notification for dependant service contains a non-null value, it will start the target service.
   * After the notification is handled, it checks if all dependentServices are not null. If they
   * are, the client's provided {@link this#createService(List)} is called, and the service is
   * registered. It will not register the service if it was already registered in the {@link
   * this#start(BundleContext)}.
   */
  @Override
  public synchronized void serviceChanged(ServiceEvent event) {
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

  private synchronized void startServiceInternal(List<Object> dependentServices) {
    if (started) {
      logger.info("The {} is already started. Ignoring the start request.", activatorName);
      return;
    }
    started = true;
    ServiceAndProperties service = createService(dependentServices);
    context.registerService(targetServiceClass.getName(), service.service, service.properties);
    logger.info("Started {}....", activatorName);
  }

  /**
   * Clients should override this method to create the Service that will be registered in the OSGi
   * container. The dependant services will contain all services registered passed to the
   * constructor of this class as {@code List<Class<?>> dependentServices}. The ordering will be the
   * same. You can safely cast the objects to the expected types according to the dependentServices.
   *
   * @return ServiceAndProperties that has the service for OSGi registration and the properties that
   *     will be passed.
   */
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
