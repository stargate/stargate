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

import java.util.*;
import java.util.Hashtable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Filter;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceReference;
import org.osgi.framework.ServiceRegistration;
import org.osgi.util.tracker.ServiceTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseActivator implements BundleActivator {
  private static final Logger logger = LoggerFactory.getLogger(BaseActivator.class);

  private final String activatorName;

  protected BundleContext context;

  public boolean started;

  public Tracker tracker;

  private List<ServiceRegistration<?>> targetServiceRegistrations = new ArrayList<>();

  /** @param activatorName - The name used when logging the progress of registration. */
  public BaseActivator(String activatorName) {
    this.activatorName = activatorName;
  }

  /**
   * When the OSGi invokes the start() method, it constructs the {@link Tracker}. It will listen for
   * notification of all services passed as {@link this#dependencies()}. It will wait for a
   * notification denoting that service was registered using {@link
   * Tracker#addingService(ServiceReference)}. If all services are present, it will call the
   * user-provided {@link this#createServices()} and register it in the OSGi using {@link
   * BundleContext#registerService(Class, Object, java.util.Dictionary)} if {@code
   * targetServiceClass.isPresent()}. If it is not present, the {@link this#createServices()} is
   * called but there will bo no registration in the OSGi.
   */
  @Override
  public synchronized void start(BundleContext context) throws InvalidSyntaxException {
    logger.info("Starting {} ...", activatorName);
    this.context = context;
    if (dependencies().isEmpty() && lazyDependencies().isEmpty()) {
      startServiceInternal();
    } else {
      tracker = new Tracker(context, context.createFilter(constructDependenciesFilter()));
      tracker.open();
    }
  }

  String constructDependenciesFilter() {
    StringBuilder builder = new StringBuilder("(|");

    List<Service<?>> allDependencies = new ArrayList<>();
    allDependencies.addAll(dependencies());
    allDependencies.addAll(lazyDependencies());

    for (Service<?> servicePointer : allDependencies) {
      if (servicePointer.identifier.isPresent()) {
        builder.append(String.format("(%s)", servicePointer.identifier.get()));
      } else {
        builder.append(String.format("(objectClass=%s)", servicePointer.expectedClass.getName()));
      }
    }
    builder.append(")");
    return builder.toString();
  }

  /**
   * It is calling the user-provided {@link this#stopService()} if the service was started
   * successfully. If and there was an OSGi service registration it will deregister service calling
   * {@link BundleContext#ungetService(ServiceReference)}.
   */
  @Override
  public synchronized void stop(BundleContext context) throws Exception {
    if (started) {
      logger.info("Stopping {}", activatorName);
      stopService();
      deregisterService();
    }
    tracker.close();
  }

  private void deregisterService() {
    for (ServiceRegistration<?> serviceRegistration : targetServiceRegistrations) {
      if (serviceRegistration != null) {
        ServiceReference<?> reference = serviceRegistration.getReference();
        logger.info("Unget service {} from {}", reference.getBundle(), activatorName);
        context.ungetService(reference);
      }
    }
  }

  private synchronized void startServiceInternal() {
    if (started) {
      logger.info("The {} is already started. Ignoring the start request.", activatorName);
      return;
    }
    started = true;
    List<ServiceAndProperties> services = createServices();
    for (ServiceAndProperties service : services) {
      if (service != null) {
        logger.info("Registering {} as {}", activatorName, service.targetServiceClass.getName());
        targetServiceRegistrations.add(
            context.registerService(
                service.targetServiceClass.getName(), service.service, service.properties));
      }
    }
    logger.info("Started {}", activatorName);
  }

  public class Tracker extends ServiceTracker<Object, Object> {

    public Tracker(BundleContext context, Filter filter) {
      super(context, filter, null);
    }

    /**
     * It will try to match all {@link this#dependencies()} with a ServiceReference notification.
     * After the notification is handled, it checks if all services in dependencies() are not null.
     * If they are not, the client's provided {@link this#createServices()} is called, and the
     * service is registered. It will not register the service if it was already registered.
     */
    @Override
    public Object addingService(ServiceReference<Object> ref) {
      Object service = super.addingService(ref);
      startIfAllRegistered(ref, service);

      return service;
    }

    public void startIfAllRegistered(ServiceReference<Object> ref, Object service) {
      if (service == null) {
        return;
      }
      for (ServicePointer<?> servicePointer : dependencies()) {
        if (servicePointer.expectedClass.isAssignableFrom(service.getClass())) {
          logger.debug("{} using service: {}", activatorName, ref.getBundle());
          servicePointer.set(service);
        }
      }

      for (LazyServicePointer<?> lazyServicePointer : lazyDependencies()) {
        if (lazyServicePointer.expectedClass.isAssignableFrom(service.getClass())) {
          logger.debug("{} using service with lazy init: {}", activatorName, ref.getBundle());
          lazyServicePointer.set(service);
        }
      }

      if (dependencies().stream().map(v -> v.service).allMatch(Objects::nonNull)) {
        startServiceInternal();
      }
    }
  }

  /**
   * Clients should override this method to create multiple Services that may be be registered in
   * the OSGi container.
   *
   * @return list of services that has the service for OSGi registration and the properties that
   *     will be passed.
   */
  protected List<ServiceAndProperties> createServices() {
    return Collections.singletonList(createService());
  }

  /**
   * Clients should override this method to create the Service that may be be registered in the OSGi
   * container. The dependent services will contain all services registered by the {@link
   * this#dependencies()}.
   *
   * @return ServiceAndProperties that has the service for OSGi registration and the properties that
   *     will be passed or null if there is no registration service required.
   */
  protected ServiceAndProperties createService() {
    return null;
  }

  /**
   * It will be called when the OSGi calls {@link this#stop(BundleContext)} and only if service was
   * already started.
   */
  protected abstract void stopService();

  /**
   * @return List of dependent services that this component relies on. It provides the
   *     happens-before meaning that all dependent services must be present before the {@link
   *     this#createServices()} is called.
   */
  protected abstract List<ServicePointer<?>> dependencies();

  /**
   * @return List of dependent services that this component relies on. It does not wait for them to
   *     be available before calling {@link this#createServices()}. The init of those services is
   *     lazy, meaning that they can be set after the {@link this#createServices()} is called.
   */
  protected List<LazyServicePointer<?>> lazyDependencies() {
    return Collections.emptyList();
  }

  public static class ServiceAndProperties {
    private final Object service;
    private final Class<?> targetServiceClass;

    private final Hashtable<String, String> properties;

    public ServiceAndProperties(
        Object service, Class<?> targetServiceClass, Hashtable<String, String> properties) {
      this.service = service;
      this.targetServiceClass = targetServiceClass;
      this.properties = properties;
    }

    @SuppressWarnings("JdkObsolete")
    public ServiceAndProperties(Object service, Class<?> targetServiceClass) {
      this(service, targetServiceClass, new Hashtable<>());
    }
  }

  private abstract static class Service<T> {
    protected Class<T> expectedClass;

    protected Optional<String> identifier;

    protected static String createIdentifier(String identifierKey, String identifierValue) {
      return String.format("%s=%s", identifierKey, identifierValue);
    }
  }

  public static class ServicePointer<T> extends Service<T> {
    private T service;

    private ServicePointer(Class<T> expectedClass, String identifier) {
      this.expectedClass = expectedClass;
      this.identifier = Optional.ofNullable(identifier);
    }

    public static <T> ServicePointer<T> create(
        Class<T> className, String identifierKey, String identifierValue) {
      return new ServicePointer<>(className, createIdentifier(identifierKey, identifierValue));
    }

    public static <T> ServicePointer<T> create(Class<T> className) {
      return new ServicePointer<>(className, null);
    }

    public T get() {
      return service;
    }

    @SuppressWarnings("unchecked")
    private void set(Object service) {
      this.service = (T) service;
    }
  }

  public static class LazyServicePointer<T> extends Service<T> {
    private AtomicReference<T> service = new AtomicReference<>();

    private LazyServicePointer(Class<T> expectedClass, String identifier) {
      this.expectedClass = expectedClass;
      this.identifier = Optional.ofNullable(identifier);
    }

    public static <T> LazyServicePointer<T> create(
        Class<T> className, String identifierKey, String identifierValue) {
      return new LazyServicePointer<>(className, createIdentifier(identifierKey, identifierValue));
    }

    public static <T> LazyServicePointer<T> create(Class<T> className) {
      return new LazyServicePointer<>(className, null);
    }

    public AtomicReference<T> get() {
      return service;
    }

    @SuppressWarnings("unchecked")
    private void set(Object service) {
      this.service.set((T) service);
    }
  }
}
