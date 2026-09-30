/*
 * Copyright 2017 VMware, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.micrometer.jersey2.server;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;

/** Vendored from micrometer-jersey2 1.8.x to avoid the removed artifact dependency. */
public interface AnnotationFinder {

  AnnotationFinder DEFAULT = new AnnotationFinder() {};

  /**
   * The default implementation performs a simple search for a declared annotation matching the
   * search type.
   */
  @SuppressWarnings("unchecked")
  default <A extends Annotation> A findAnnotation(
      AnnotatedElement annotatedElement, Class<A> annotationType) {
    Annotation[] anns = annotatedElement.getDeclaredAnnotations();
    for (Annotation ann : anns) {
      if (ann.annotationType() == annotationType) {
        return (A) ann;
      }
    }
    return null;
  }
}
