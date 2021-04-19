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
import java.util.stream.Collectors;

public class MutationMappingModelFactory {

  public static MutationMappingModel build(
      FieldDefinition mutation,
      String parentTypeName,
      Map<String, EntityMappingModel> entities,
      Map<String, ResponseMappingModel> responses,
      ProcessingContext context)
      throws SkipException {
    return detectType(mutation, context)
        .getBuilder(mutation, parentTypeName, entities, responses, context)
        .build();
  }

  private static Kind detectType(FieldDefinition mutation, ProcessingContext context)
      throws SkipException {
    for (Kind kind : Kind.values()) {
      if (mutation.hasDirective(kind.getDirectiveName())) {
        return kind;
      }
    }
    for (Kind kind : Kind.values()) {
      for (String prefix : kind.getPrefixes()) {
        if (mutation.getName().startsWith(prefix)) {
          context.addInfo(
              mutation.getSourceLocation(),
              "Mutation %s: mapping to a CQL %s because it starts with '%s'",
              mutation.getName(),
              kind,
              prefix);
          return kind;
        }
      }
    }
    context.addError(
        mutation.getSourceLocation(),
        ProcessingErrorType.InvalidMapping,
        "Mutation %s: could not infer mutation kind. Either use one of the mutation "
            + "directives (%s), or name your operation with a recognized prefix.",
        mutation.getName(),
        Arrays.stream(Kind.values()).map(Kind::getDirectiveName).collect(Collectors.joining(", ")));
    throw SkipException.INSTANCE;
  }

  /**
   * The type of mutation that a GraphQL operation is mapped to. It's inferred either from an
   * explicit directive, or otherwise from a set of predefined name prefixes.
   */
  private enum Kind {
    INSERT(InsertMappingModelBuilder::new, "cql_insert", "insert", "create"),
    UPDATE(UpdateMappingModelBuilder::new, "cql_update", "update"),
    DELETE(DeleteMappingModelBuilder::new, "cql_delete", "delete", "remove"),
    ;

    private final BuilderProvider builderProvider;
    private final String directiveName;
    private final Iterable<String> prefixes;

    Kind(BuilderProvider builderProvider, String directiveName, String... prefixes) {
      this.builderProvider = builderProvider;
      this.directiveName = directiveName;
      this.prefixes = ImmutableList.copyOf(prefixes);
    }

    String getDirectiveName() {
      return directiveName;
    }

    Iterable<String> getPrefixes() {
      return prefixes;
    }

    MutationMappingModelBuilder getBuilder(
        FieldDefinition mutation,
        String parentTypeName,
        Map<String, EntityMappingModel> entities,
        Map<String, ResponseMappingModel> responses,
        ProcessingContext context) {
      return builderProvider.get(mutation, parentTypeName, entities, responses, context);
    }
  }

  @FunctionalInterface
  interface BuilderProvider {
    MutationMappingModelBuilder get(
        FieldDefinition mutation,
        String parentTypeName,
        Map<String, EntityMappingModel> entities,
        Map<String, ResponseMappingModel> responses,
        ProcessingContext context);
  }
}
