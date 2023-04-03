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

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Defines how the Stargate node(s) required by a test should be configured.
 *
 * <p><b>IMPORTANT:</b> When defining custom Stargate spec, please set <code>@Order(TestOrder.LAST)
 * </code> on the test class, to ensure test is invoked last. Otherwise, spinning up and down the
 * context for that test class would slow down the CI.
 */
@Target({ElementType.TYPE, ElementType.METHOD})
@Inherited
@Retention(RetentionPolicy.RUNTIME)
public @interface StargateSpec {

  /**
   * Indicates whether Stargate instances started from this specification can be shared among
   * different tests.
   */
  boolean shared() default true;

  /**
   * Defines the name of a method (static or instance depending on the target of this annotation)
   * that will be called on parameter of type {@link StargateParameters.Builder} before starting
   * Stargate nodes.
   *
   * @return method name
   */
  String parametersCustomizer() default "";

  /** Defines the number of Stargate nodes required by the test. */
  int nodes() default 1;
}
