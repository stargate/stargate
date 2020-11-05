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

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import java.util.Hashtable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Filter;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceReference;
import org.osgi.util.tracker.ServiceTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseActivator implements BundleActivator {
  private static final Logger logger = LoggerFactory.getLogger(BaseActivator.class);

  private final String activatorName;

  protected BundleContext context;

  private Optional<Class<?>> targetServiceClass;

  public boolean started;

  @VisibleForTesting
  @GuardedBy("this")
  public Tracker tracker;

  /**
   * @param activatorName - The name used when logging the progress of registration.
   * @param targetServiceClass - This class will be used when registering the service. If null, then
   *     the registration will not happen.
   */
  public BaseActivator(String activatorName, @Nullable Class<?> targetServiceClass) {
    this.activatorName = activatorName;
    this.targetServiceClass = Optional.ofNullable(targetServiceClass);
  }

  /**
   * Convenience method for activators that does not register any service see docs for {@link
   * this#BaseActivator(String, Class)}.
   */
  public BaseActivator(String activatorName) {
    this(activatorName, null);
  }

  /**
   * When the OSGi invokes the start() method, it constructs the {@link Tracker}. It will listen for
   * notification of all services passed as {@link this#dependencies()}. It will wait for a
   * notification denoting that service was registered using {@link
   * Tracker#addingService(ServiceReference)}. If all services are present, it will call the
   * user-provided {@link this#createService()} and register it in the OSGi using {@link
   * BundleContext#registerService(Class, Object, java.util.Dictionary)} if {@code
   * targetServiceClass.isPresent()}. If it is not present, the {@link this#createService()} is
   * called but there will bo no registration in the OSGi.
   */
  @Override
  public synchronized void start(BundleContext context) throws InvalidSyntaxException {
    logger.info("Starting {} ...", activatorName);
    this.context = context;
    tracker = new Tracker(context, context.createFilter(constructDependenciesFilter()));
    tracker.open();
  }

  @VisibleForTesting
  String constructDependenciesFilter() {
    StringBuilder builder = new StringBuilder("(|");

    for (ServicePointer<?> servicePointer : dependencies()) {
      if (servicePointer.identifier.isPresent()) {
        builder.append(String.format("(%s)", servicePointer.identifier.get()));
      } else {
        builder.append(String.format("(objectClass=%s)", servicePointer.className.getName()));
      }
    }
    builder.append(")");
    return builder.toString();
  }

  @Override
  public synchronized void stop(BundleContext context) throws Exception {
    if (started) {
      logger.info("Stopping {}", activatorName);
      stopService();
    }
    tracker.close();
  }

  private synchronized void startServiceInternal() {
    if (started) {
      logger.info("The {} is already started. Ignoring the start request.", activatorName);
      return;
    }
    started = true;
    ServiceAndProperties service = createService();
    if (service != null && targetServiceClass.isPresent()) {
      logger.info("Registering {} as {}", activatorName, targetServiceClass.get().getName());
      context.registerService(
          targetServiceClass.get().getName(), service.service, service.properties);
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
     * If they are not, the client's provided {@link this#createService()} is called, and the
     * service is registered. It will not register the service if it was already registered.
     */
    @Override
    public Object addingService(ServiceReference<Object> ref) {
      Object service = super.addingService(ref);
      startIfAllRegistered(ref, service);

      return service;
    }

    @VisibleForTesting
    public void startIfAllRegistered(ServiceReference<Object> ref, Object service) {
      if (service == null) {
        return;
      }
      for (ServicePointer<?> servicePointer : dependencies()) {
        if (servicePointer.className.isAssignableFrom(service.getClass())) {
          logger.debug("{} using service: {}", activatorName, ref.getBundle());
          servicePointer.setService(service);
        }
      }
      if (dependencies().stream().map(v -> v.service).allMatch(Objects::nonNull)) {
        startServiceInternal();
      }
    }
  }

  /**
   * Clients should override this method to create the Service that may be be registered in the OSGi
   * container. The dependent services will contain all services registered by the {@link
   * this#dependencies()}.
   *
   * @return ServiceAndProperties that has the service for OSGi registration and the properties that
   *     will be passed or null if there is no registration service required.
   */
  @Nullable
  protected abstract ServiceAndProperties createService();

  /**
   * It will be called when the OSGi calls {@link this#stop(BundleContext)} and only if service was
   * already started.
   */
  protected abstract void stopService();

  /**
   * @return List of dependent services that this component relies on. It provides the
   *     happens-before meaning that all dependent services must be present before the {@link
   *     this#createService()} is called.
   */
  protected abstract List<ServicePointer<?>> dependencies();

  public static class ServiceAndProperties {
    private final Object service;

    private final Hashtable<String, String> properties;

    public ServiceAndProperties(Object service, Hashtable<String, String> properties) {
      this.service = service;
      this.properties = properties;
    }
  }

  public static class ServicePointer<T> {
    private Class<T> className;

    private Optional<String> identifier;

    private T service;

    private ServicePointer(Class<T> className, @Nullable String identifier) {
      this.className = className;
      this.identifier = Optional.ofNullable(identifier);
    }

    public static <T> ServicePointer<T> create(
        Class<T> className, String identifierKey, String identifierValue) {
      return new ServicePointer<>(
          className, String.format("%s=%s", identifierKey, identifierValue));
    }

    public static <T> ServicePointer<T> create(Class<T> className) {
      return new ServicePointer<>(className, null);
    }

    public T getService() {
      return service;
    }

    @SuppressWarnings("unchecked")
    private void setService(Object service) {
      this.service = (T) service;
    }
  }
}
