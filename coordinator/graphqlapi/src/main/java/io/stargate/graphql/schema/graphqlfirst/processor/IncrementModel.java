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
package io.stargate.graphql.schema.graphqlfirst.processor;

public class IncrementModel {
  private final FieldModel field;
  private final boolean prepend;
  private final String argumentName;

  public IncrementModel(FieldModel field, boolean prepend, String argumentName) {
    this.field = field;
    this.prepend = prepend;
    this.argumentName = argumentName;
  }

  public FieldModel getField() {
    return field;
  }

  public boolean isPrepend() {
    return prepend;
  }

  /** The name of the argument that will contain the value to bind. */
  public String getArgumentName() {
    return argumentName;
  }
}
