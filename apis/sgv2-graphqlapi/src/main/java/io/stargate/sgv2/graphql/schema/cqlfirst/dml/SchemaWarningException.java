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
package io.stargate.sgv2.graphql.schema.cqlfirst.dml;

/**
 * When a warning occurs deep in the schema conversion logic, it's sometimes easier to throw an
 * exception to interrupt the flow. We use that type to signal that the top-level code doesn't need
 * to log the stack trace.
 */
public class SchemaWarningException extends RuntimeException {
  public SchemaWarningException(String message) {
    super(message);
  }
}
