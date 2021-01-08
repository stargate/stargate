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
package io.stargate.graphql.schema.schemafirst.processor;

import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;
import graphql.language.SourceLocation;
import graphql.schema.idl.TypeDefinitionRegistry;
import io.stargate.db.schema.Keyspace;
import java.util.ArrayList;
import java.util.List;

class ProcessingContext {

  private final TypeDefinitionRegistry typeRegistry;
  private final Keyspace keyspace;
  private final List<ProcessingMessage> messages;
  private final List<ProcessingMessage> errors;

  ProcessingContext(TypeDefinitionRegistry typeRegistry, Keyspace keyspace) {
    this.typeRegistry = typeRegistry;
    this.keyspace = keyspace;
    this.messages = new ArrayList<>();
    this.errors = new ArrayList<>();
  }

  /** The types that were initially parsed from the schema. Useful to look up type references. */
  TypeDefinitionRegistry getTypeRegistry() {
    return typeRegistry;
  }

  /** The keyspace that the schema will be deployed to. */
  Keyspace getKeyspace() {
    return keyspace;
  }

  @FormatMethod
  void addInfo(SourceLocation location, @FormatString String format, Object... arguments) {
    messages.add(new ProcessingMessage(location, ProcessingMessageType.Info, format, arguments));
  }

  @FormatMethod
  void addWarning(SourceLocation location, @FormatString String format, Object... arguments) {
    messages.add(new ProcessingMessage(location, ProcessingMessageType.Warning, format, arguments));
  }

  @FormatMethod
  void addError(
      SourceLocation location,
      ProcessingMessageType messageType,
      @FormatString String format,
      Object... arguments) {
    errors.add(new ProcessingMessage(location, messageType, format, arguments));
  }

  /** Info or warning messages that will be included in the response to the deploy operation. */
  List<ProcessingMessage> getMessages() {
    return messages;
  }

  /**
   * Any errors encountered during processing. We'll eventually throw an exception, but we want to
   * detect as many errors as possible.
   */
  List<ProcessingMessage> getErrors() {
    return errors;
  }
}
