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
package io.stargate.it.http;

import java.lang.annotation.*;

/** Defines how the REST API node(s) required by a test should be configured. */
@Target({ElementType.TYPE, ElementType.METHOD})
@Inherited
@Retention(RetentionPolicy.RUNTIME)
public @interface ApiServiceSpec {

  /**
   * Indicates whether REST API instances started from this specification can be shared among
   * different tests.
   */
  boolean shared() default true; // assume stateless invocations

  /**
   * Defines the name of a method (static or instance depending on the target of this annotation)
   * that will be called on parameter of type {@link ApiServiceParameters.Builder} before starting
   * REST API instances.
   *
   * @return method name
   */
  String parametersCustomizer() default "";
}
