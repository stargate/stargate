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

import com.google.common.collect.ImmutableMap;
import graphql.Scalars;
import graphql.language.Directive;
import graphql.language.EnumTypeDefinition;
import graphql.language.FieldDefinition;
import graphql.language.ListType;
import graphql.language.NonNullType;
import graphql.language.ObjectTypeDefinition;
import graphql.language.Type;
import graphql.language.TypeDefinition;
import graphql.language.TypeName;
import graphql.schema.idl.TypeDefinitionRegistry;
import io.stargate.grpc.TypeSpecs;
import io.stargate.proto.QueryOuterClass.TypeSpec;
import io.stargate.proto.Schema.ColumnOrderBy;
import io.stargate.sgv2.graphql.schema.scalars.CqlScalar;
import java.util.Map;
import java.util.Optional;

class FieldModelBuilder extends ModelBuilderBase<FieldModel> {

  private static final Map<String, TypeSpec> GRAPHQL_SCALAR_MAPPINGS =
      ImmutableMap.<String, TypeSpec>builder()
          .put(Scalars.GraphQLInt.getName(), TypeSpecs.INT)
          .put(Scalars.GraphQLFloat.getName(), TypeSpecs.DOUBLE)
          .put(Scalars.GraphQLString.getName(), TypeSpecs.VARCHAR)
          .put(Scalars.GraphQLBoolean.getName(), TypeSpecs.BOOLEAN)
          .put(Scalars.GraphQLID.getName(), TypeSpecs.UUID)
          .build();

  private final FieldDefinition field;
  private final String parentCqlName;
  private final EntityModel.Target targetContainer;
  private final boolean checkForInputType;
  private final String graphqlName;
  private final Type<?> graphqlType;
  private final String messagePrefix;
  private final Optional<Directive> cqlColumnDirective;

  FieldModelBuilder(
      FieldDefinition field,
      ProcessingContext context,
      String parentCqlName,
      String parentGraphqlName,
      EntityModel.Target targetContainer,
      boolean checkForInputType) {

    super(context, field.getSourceLocation());

    this.field = field;
    this.parentCqlName = parentCqlName;
    this.targetContainer = targetContainer;
    this.checkForInputType = checkForInputType;

    this.graphqlName = field.getName();
    this.graphqlType = field.getType();
    this.messagePrefix = parentGraphqlName + "." + graphqlName;
    this.cqlColumnDirective = DirectiveHelper.getDirective(CqlDirectives.COLUMN, field);
  }

  @Override
  FieldModel build() throws SkipException {
    String cqlName =
        cqlColumnDirective
            .flatMap(d -> DirectiveHelper.getStringArgument(d, CqlDirectives.COLUMN_NAME, context))
            .orElse(graphqlName);
    boolean isUdtField = targetContainer == EntityModel.Target.UDT;
    boolean partitionKey = isPartitionKey(isUdtField);
    Optional<ColumnOrderBy> clusteringOrder = getClusteringOrder(isUdtField);

    if (partitionKey && clusteringOrder.isPresent()) {
      invalidMapping("%s: can't be both a partition key and a clustering key.", messagePrefix);
      throw SkipException.INSTANCE;
    }

    boolean isPk = partitionKey || clusteringOrder.isPresent();

    TypeSpec cqlType = inferCqlType(graphqlType, true, context);
    if (isPk || (cqlType.hasUdt() && isUdtField)) {
      cqlType = TypeSpecs.freeze(cqlType);
    }

    cqlType = maybeUseTypeHint(cqlType, isPk, isUdtField);

    return new FieldModel(
        graphqlName,
        graphqlType,
        cqlName,
        cqlType,
        partitionKey,
        clusteringOrder,
        getIndex(cqlName, isUdtField, isPk, cqlType));
  }

  private Boolean isPartitionKey(boolean isUdtField) {
    return cqlColumnDirective
        .flatMap(
            d -> DirectiveHelper.getBooleanArgument(d, CqlDirectives.COLUMN_PARTITION_KEY, context))
        .filter(
            __ -> {
              if (isUdtField) {
                warn(
                    "%s: UDT fields should not be marked as partition keys (this will be ignored)",
                    messagePrefix);
                return false;
              }
              return true;
            })
        .orElse(false);
  }

  private Optional<ColumnOrderBy> getClusteringOrder(boolean isUdtField) {
    return cqlColumnDirective
        .flatMap(
            d ->
                DirectiveHelper.getEnumArgument(
                    d, CqlDirectives.COLUMN_CLUSTERING_ORDER, ColumnOrderBy.class, context))
        .filter(
            __ -> {
              if (isUdtField) {
                warn(
                    "%s: UDT fields should not be marked as clustering keys (this will be ignored)",
                    messagePrefix);
                return false;
              }
              return true;
            });
  }

  /** Computes the CQL type that corresponds to the GraphQL type of a field. */
  private TypeSpec inferCqlType(Type<?> graphqlType, boolean isRoot, ProcessingContext context)
      throws SkipException {
    if (graphqlType instanceof NonNullType) {
      return inferCqlType(((NonNullType) graphqlType).getType(), isRoot, context);
    } else if (graphqlType instanceof ListType) {
      return TypeSpec.newBuilder()
          .setList(
              TypeSpec.List.newBuilder()
                  .setElement(inferCqlType(((ListType) graphqlType).getType(), false, context))
                  .setFrozen(!isRoot))
          .build();
    } else {
      String typeName = ((TypeName) graphqlType).getName();
      TypeDefinitionRegistry typeRegistry = context.getTypeRegistry();

      // Check built-in GraphQL scalars
      if (GRAPHQL_SCALAR_MAPPINGS.containsKey(typeName)) {
        return GRAPHQL_SCALAR_MAPPINGS.get(typeName);
      }

      // Otherwise, check if the type references another definition in the user's schema
      if (typeRegistry.types().containsKey(typeName)) {
        TypeDefinition<?> definition =
            typeRegistry.getType(typeName).orElseThrow(AssertionError::new);
        if (definition instanceof EnumTypeDefinition) {
          return TypeSpecs.VARCHAR;
        }
        if (definition instanceof ObjectTypeDefinition) {
          return expectUdt((ObjectTypeDefinition) definition, isRoot);
        }
        invalidMapping("%s: can't map type '%s' to CQL", messagePrefix, graphqlType);
        throw SkipException.INSTANCE;
      }

      // Otherwise, check our own CQL scalars
      // Note that we do this last in case the user defined an object or enum type that happens to
      // have the same name as a CQL scalar.
      Optional<CqlScalar> maybeCqlScalar = CqlScalar.fromGraphqlName(typeName);
      if (maybeCqlScalar.isPresent()) {
        CqlScalar cqlScalar = maybeCqlScalar.get();
        // Remember that we'll need to add the scalar to the RuntimeWiring
        context.getUsedCqlScalars().add(cqlScalar);
        return cqlScalar.getCqlType();
      }

      invalidMapping("%s: can't map type '%s' to CQL", messagePrefix, graphqlType);
      throw SkipException.INSTANCE;
    }
  }

  /** Checks that if a field is an object type, then that object maps to a UDT. */
  private TypeSpec expectUdt(ObjectTypeDefinition definition, boolean isRoot) throws SkipException {
    boolean isUdt =
        DirectiveHelper.getDirective(CqlDirectives.ENTITY, definition)
            .flatMap(
                d ->
                    DirectiveHelper.getEnumArgument(
                        d, CqlDirectives.ENTITY_TARGET, EntityModel.Target.class, context))
            .filter(target -> target == EntityModel.Target.UDT)
            .isPresent();
    if (isUdt) {
      if (checkForInputType
          && !DirectiveHelper.getDirective(CqlDirectives.INPUT, definition).isPresent()) {
        invalidMapping(
            "%s: type '%s' must also be annotated with @%s",
            messagePrefix, definition.getName(), CqlDirectives.INPUT);
        throw SkipException.INSTANCE;
      }
      return TypeSpec.newBuilder()
          .setUdt(TypeSpec.Udt.newBuilder().setName(definition.getName()).setFrozen(!isRoot))
          .build();
    }

    invalidMapping(
        "%s: can't map type '%s' to CQL -- "
            + "if a field references an object type, then that object should map to a UDT",
        messagePrefix, definition.getName());
    throw SkipException.INSTANCE;
  }

  /**
   * If the directive provides a CQL type hint, use it, provided that it is valid and compatible
   * with the inferred type.
   */
  private TypeSpec maybeUseTypeHint(TypeSpec cqlType, boolean isPk, boolean isUdtField)
      throws SkipException {
    Optional<String> maybeCqlTypeHint =
        cqlColumnDirective.flatMap(
            d -> DirectiveHelper.getStringArgument(d, CqlDirectives.COLUMN_TYPE_HINT, context));
    if (!maybeCqlTypeHint.isPresent()) {
      return cqlType;
    }

    TypeSpec cqlTypeHint;
    String spec = maybeCqlTypeHint.get();
    try {
      cqlTypeHint = TypeSpecs.parse(spec, context.getKeyspace().getTypesList(), false);
    } catch (IllegalArgumentException e) {
      invalidSyntax("%s: could not parse CQL type '%s': %s", messagePrefix, spec, e.getMessage());
      throw SkipException.INSTANCE;
    }
    if (isPk
        && (TypeSpecs.isCollection(cqlTypeHint) || cqlTypeHint.hasUdt())
        && !TypeSpecs.isFrozen(cqlTypeHint)) {
      invalidMapping(
          "%s: invalid type hint '%s' -- partition or clustering columns must be frozen",
          messagePrefix, spec);
      throw SkipException.INSTANCE;
    }
    if (cqlTypeHint.hasUdt() && isUdtField && !TypeSpecs.isFrozen(cqlTypeHint)) {
      invalidMapping(
          "%s: invalid type hint '%s' -- nested UDTs must be frozen", messagePrefix, spec);
      throw SkipException.INSTANCE;
    }
    if (!isCompatible(cqlTypeHint, cqlType)) {
      invalidMapping(
          "%s: invalid type hint '%s'-- the inferred type was '%s', you can only change "
              + "frozenness or use sets instead of lists",
          messagePrefix, TypeSpecs.format(cqlTypeHint), TypeSpecs.format(cqlType));
      throw SkipException.INSTANCE;
    }
    return cqlTypeHint;
  }

  /** Checks if the only differences are frozen vs. not frozen, or list vs. set. */
  private static boolean isCompatible(TypeSpec type1, TypeSpec type2) {
    TypeSpec.SpecCase spec1 = type1.getSpecCase();
    TypeSpec.SpecCase spec2 = type2.getSpecCase();
    if (spec1 != spec2
        && !(spec1 == TypeSpec.SpecCase.LIST && spec2 == TypeSpec.SpecCase.SET)
        && !(spec1 == TypeSpec.SpecCase.SET && spec2 == TypeSpec.SpecCase.LIST)) {
      return false;
    }
    switch (spec1) {
      case UDT:
        return type1.getUdt().getName().equals(type2.getUdt().getName());
      case LIST:
      case SET:
        TypeSpec element1 =
            spec1 == TypeSpec.SpecCase.LIST
                ? type1.getList().getElement()
                : type1.getSet().getElement();
        TypeSpec element2 =
            spec2 == TypeSpec.SpecCase.LIST
                ? type2.getList().getElement()
                : type2.getSet().getElement();
        return isCompatible(element1, element2);
      case MAP:
        TypeSpec.Map map1 = type1.getMap();
        TypeSpec.Map map2 = type2.getMap();
        return isCompatible(map1.getKey(), map2.getKey())
            && isCompatible(map1.getValue(), map2.getValue());
      case TUPLE:
        TypeSpec.Tuple tuple1 = type1.getTuple();
        TypeSpec.Tuple tuple2 = type2.getTuple();
        if (tuple1.getElementsCount() != tuple2.getElementsCount()) {
          return false;
        }
        for (int i = 0; i < tuple1.getElementsCount(); i++) {
          if (!isCompatible(tuple1.getElements(i), tuple2.getElements(i))) {
            return false;
          }
        }
        return true;
      default:
        return true;
    }
  }

  private Optional<IndexModel> getIndex(
      String cqlName, boolean isUdtField, boolean isPk, final TypeSpec cqlType)
      throws SkipException {
    Optional<Directive> cqlIndexDirective =
        DirectiveHelper.getDirective(CqlDirectives.INDEX, field);
    if (cqlIndexDirective.isPresent()) {
      if (isPk) {
        invalidMapping("%s: partition or clustering columns can't have an index", messagePrefix);
        throw SkipException.INSTANCE;
      }
      if (isUdtField) {
        invalidMapping("%s: UDT fields can't have an index", messagePrefix);
        throw SkipException.INSTANCE;
      }
      return Optional.of(
          new IndexModelBuilder(
                  cqlIndexDirective.get(), parentCqlName, cqlName, cqlType, messagePrefix, context)
              .build());
    }
    return Optional.empty();
  }
}
