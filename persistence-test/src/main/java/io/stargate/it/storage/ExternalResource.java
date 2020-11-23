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
package io.stargate.it.storage;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.util.Optional;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ExtensionContext.Store;
import org.junit.platform.commons.support.AnnotationSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ExternalResource<A extends Annotation, R extends ExternalResource.Holder>
    implements BeforeAllCallback, BeforeEachCallback {

  private static final Logger LOG = LoggerFactory.getLogger(ExternalResource.class);

  private final Class<A> annotationClass;
  private final String key;
  private final ExtensionContext.Namespace namespace;

  protected ExternalResource(Class<A> annotationClass, String key, Namespace namespace) {
    this.annotationClass = annotationClass;
    this.key = key;
    this.namespace = namespace;
  }

  protected abstract boolean isShared(A annotation);

  protected abstract Optional<R> processResource(
      R existingResource, A annotation, ExtensionContext context) throws Exception;

  protected Optional<R> getResource(ExtensionContext context) {
    Store store = context.getStore(namespace);
    //noinspection unchecked
    return Optional.ofNullable((R) store.get(key));
  }

  private void process(ExtensionContext context, ExtensionContext destination, A spec)
      throws Exception {
    R existing = getResource(context).orElse(null);
    Optional<R> r = processResource(existing, spec, context);
    if (r.isPresent()) {
      Store store = destination.getStore(namespace);

      Holder h = r.get();
      h.init(store, key, getClass().getSimpleName(), destination.getUniqueId());

      store.put(key, h);

      LOG.info(
          "Created resource for {} in {} for {}",
          getClass().getSimpleName(),
          destination.getUniqueId(),
          context.getUniqueId());
    }
  }

  private void process(ExtensionContext context) throws Exception {
    Optional<AnnotatedElement> element = context.getElement();
    Optional<A> annotation = AnnotationSupport.findAnnotation(element, annotationClass);

    if (annotation.isPresent()) {
      A spec = annotation.get();

      if (isShared(spec)) {
        // use global / root context for storing the resource
        process(context, context.getRoot(), spec);
      } else {
        process(context, context, spec);
      }
    } else {
      LOG.info("Ignoring null {} spec in {}", getClass().getSimpleName(), context.getUniqueId());
    }
  }

  @Override
  public void beforeAll(ExtensionContext context) throws Exception {
    process(context);
  }

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    process(context);
  }

  public abstract static class Holder implements Store.CloseableResource {
    private String location;
    private String type;
    private Store store;
    private String key;

    private void init(Store store, String key, String type, String location) {
      this.store = store;
      this.key = key;
      this.type = type;
      this.location = location;
    }

    @Override
    public void close() {
      LOG.info("Closing {} resource in {}", type, location);

      if (store != null) {
        store.remove(key);
      }
    }
  }
}
