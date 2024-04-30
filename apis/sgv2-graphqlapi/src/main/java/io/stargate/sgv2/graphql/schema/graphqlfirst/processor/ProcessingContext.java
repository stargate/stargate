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
package io.stargate.sgv2.graphql.schema.graphqlfirst.processor;

import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;
import graphql.GraphQL;
import graphql.language.SourceLocation;
import graphql.schema.idl.TypeDefinitionRegistry;
import io.stargate.bridge.proto.Schema.CqlKeyspaceDescribe;
import io.stargate.sgv2.api.common.grpc.StargateBridgeClient;
import io.stargate.sgv2.graphql.schema.scalars.CqlScalar;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

class ProcessingContext {

  private final TypeDefinitionRegistry typeRegistry;
  private final CqlKeyspaceDescribe keyspace;
  private final StargateBridgeClient bridge;
  private final boolean isPersisted;
  private final EnumSet<CqlScalar> usedCqlScalars = EnumSet.noneOf(CqlScalar.class);
  private final List<ProcessingMessage<ProcessingLogType>> logs;
  private final List<ProcessingMessage<ProcessingErrorType>> errors;

  ProcessingContext(
      TypeDefinitionRegistry typeRegistry,
      CqlKeyspaceDescribe keyspace,
      StargateBridgeClient bridge,
      boolean isPersisted) {
    this.typeRegistry = typeRegistry;
    this.keyspace = keyspace;
    this.bridge = bridge;
    this.isPersisted = isPersisted;
    this.logs = new ArrayList<>();
    this.errors = new ArrayList<>();
  }

  /** The types that were initially parsed from the schema. Useful to look up type references. */
  TypeDefinitionRegistry getTypeRegistry() {
    return typeRegistry;
  }

  /** The keyspace that the schema will be deployed to. */
  CqlKeyspaceDescribe getKeyspace() {
    return keyspace;
  }

  public StargateBridgeClient getBridge() {
    return bridge;
  }

  /**
   * @see SchemaProcessor#SchemaProcessor(Persistence, boolean)
   */
  public boolean isPersisted() {
    return isPersisted;
  }

  /**
   * The CQL scalars that are referenced by the schema.
   *
   * <p>We discover them while building the {@link MappingModel}. The final {@link GraphQL} only
   * contains the CQL scalars that are actually used (if any).
   */
  EnumSet<CqlScalar> getUsedCqlScalars() {
    return usedCqlScalars;
  }

  @FormatMethod
  void addInfo(SourceLocation location, @FormatString String format, Object... arguments) {
    logs.add(ProcessingMessage.log(location, ProcessingLogType.Info, format, arguments));
  }

  @FormatMethod
  void addWarning(SourceLocation location, @FormatString String format, Object... arguments) {
    logs.add(ProcessingMessage.log(location, ProcessingLogType.Warning, format, arguments));
  }

  @FormatMethod
  void addError(
      SourceLocation location,
      ProcessingErrorType messageType,
      @FormatString String format,
      Object... arguments) {
    errors.add(ProcessingMessage.error(location, messageType, format, arguments));
  }

  /** Info or warning messages that will be included in the response to the deploy operation. */
  List<ProcessingMessage<ProcessingLogType>> getLogs() {
    return logs;
  }

  /**
   * Any errors encountered during processing. We'll eventually throw an exception, but we want to
   * detect as many errors as possible.
   */
  List<ProcessingMessage<ProcessingErrorType>> getErrors() {
    return errors;
  }
}
