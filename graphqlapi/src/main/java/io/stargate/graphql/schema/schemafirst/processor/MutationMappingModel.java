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

import com.google.common.collect.ImmutableList;
import graphql.language.FieldDefinition;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public abstract class MutationMappingModel extends OperationMappingModel {

  public MutationMappingModel(String parentTypeName, FieldDefinition field) {
    super(parentTypeName, field);
  }

  public static Optional<MutationMappingModel> build(
      FieldDefinition mutation,
      String parentName,
      Map<String, EntityMappingModel> entities,
      ProcessingContext context) {
    return detectType(mutation, context)
        .flatMap(type -> type.buildModel(mutation, parentName, entities, context));
  }

  private static Optional<Kind> detectType(FieldDefinition mutation, ProcessingContext context) {
    for (Kind kind : Kind.values()) {
      if (mutation.hasDirective(kind.getDirectiveName())) {
        return Optional.of(kind);
      }
    }
    for (Kind kind : Kind.values()) {
      for (String prefix : kind.getPrefixes()) {
        if (mutation.getName().startsWith(prefix)) {
          return Optional.of(kind);
        }
      }
    }
    context.addError(
        mutation.getSourceLocation(),
        ProcessingMessageType.InvalidMapping,
        "Could not infer mutation kind. Either use one of the mutation directives (%s), "
            + "or name your operation with a recognized prefix.",
        Arrays.stream(Kind.values()).map(Kind::getDirectiveName).collect(Collectors.joining(", ")));
    return Optional.empty();
  }

  /**
   * The type of mutation that a GraphQL operation is mapped to. It's inferred either from an
   * explicit directive, or otherwise from a set of predefined name prefixes.
   */
  private enum Kind {
    INSERT("cql_insert", "insert", "create") {
      @Override
      Optional<MutationMappingModel> buildModel(
          FieldDefinition mutation,
          String parentName,
          Map<String, EntityMappingModel> entities,
          ProcessingContext context) {
        return InsertMappingModel.build(mutation, parentName, entities, context);
      }
    },
    DELETE("cql_delete", "delete", "remove") {
      @Override
      Optional<MutationMappingModel> buildModel(
          FieldDefinition mutation,
          String parentName,
          Map<String, EntityMappingModel> entities,
          ProcessingContext context) {
        throw new UnsupportedOperationException("TODO");
      }
    },
    ;

    private final String directiveName;
    private final Iterable<String> prefixes;

    Kind(String directiveName, String... prefixes) {
      this.directiveName = directiveName;
      this.prefixes = ImmutableList.copyOf(prefixes);
    }

    String getDirectiveName() {
      return directiveName;
    }

    Iterable<String> getPrefixes() {
      return prefixes;
    }

    abstract Optional<MutationMappingModel> buildModel(
        FieldDefinition mutation,
        String parentName,
        Map<String, EntityMappingModel> entities,
        ProcessingContext context);
  }
}
