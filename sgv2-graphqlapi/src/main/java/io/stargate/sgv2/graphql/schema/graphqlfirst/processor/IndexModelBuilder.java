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

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import graphql.language.Directive;
import io.stargate.grpc.TypeSpecs;
import io.stargate.proto.QueryOuterClass.TypeSpec;
import io.stargate.proto.Schema.IndexingType;
import io.stargate.proto.Schema.SupportedFeaturesResponse;
import io.stargate.sgv2.common.cql.builder.CollectionIndexingType;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

class IndexModelBuilder extends ModelBuilderBase<IndexModel> {

  private static final Splitter.MapSplitter OPTIONS_SPLITTER =
      Splitter.on(',').withKeyValueSeparator(Splitter.on(':').trimResults(CharMatcher.anyOf("' ")));

  private final Directive cqlIndexDirective;
  private final String parentCqlName;
  private final String columnCqlName;
  private final TypeSpec cqlType;
  private final String messagePrefix;

  IndexModelBuilder(
      Directive cqlIndexDirective,
      String parentCqlName,
      String columnCqlName,
      TypeSpec cqlType,
      String messagePrefix,
      ProcessingContext context) {
    super(context, cqlIndexDirective.getSourceLocation());
    this.cqlIndexDirective = cqlIndexDirective;
    this.parentCqlName = parentCqlName;
    this.columnCqlName = columnCqlName;
    this.cqlType = cqlType;
    this.messagePrefix = messagePrefix;
  }

  @Override
  IndexModel build() throws SkipException {
    String indexName =
        DirectiveHelper.getStringArgument(cqlIndexDirective, CqlDirectives.INDEX_NAME, context)
            .orElse(parentCqlName + '_' + columnCqlName + "_idx");

    Optional<String> indexClass =
        DirectiveHelper.getStringArgument(cqlIndexDirective, CqlDirectives.INDEX_CLASS, context);

    // Some persistence backends default to SAI when no class is specified:
    SupportedFeaturesResponse supportedFeatures = context.getBridge().getSupportedFeatures();
    if (!indexClass.isPresent() && !supportedFeatures.getSecondaryIndexes()) {
      if (!context.getBridge().getSupportedFeatures().getSai()) {
        // Cannot happen with any of the existing persistence implementations, but handle it just in
        // case:
        invalidMapping(
            "%s: the persistence backend does not support regular secondary indexes nor SAI, "
                + "indexes that don't specify %s can't be mapped",
            messagePrefix, CqlDirectives.INDEX_CLASS);
        throw SkipException.INSTANCE;
      }
      info(
          "%s: using SAI for index %s because the persistence backend does not support "
              + "regular secondary indexes",
          messagePrefix, indexName);
      indexClass = Optional.of(IndexModel.SAI_INDEX_CLASS_NAME);
    }

    IndexingType indexingType =
        DirectiveHelper.getEnumArgument(
                cqlIndexDirective,
                CqlDirectives.INDEX_TARGET,
                CollectionIndexingType.class,
                context)
            .filter(this::validateTarget)
            .map(this::toGrpc)
            .orElse(TypeSpecs.isCollection(cqlType) ? IndexingType.VALUES_ : IndexingType.DEFAULT);
    Map<String, String> indexOptions =
        DirectiveHelper.getStringArgument(cqlIndexDirective, CqlDirectives.INDEX_OPTIONS, context)
            .flatMap(this::convertOptions)
            .orElse(Collections.emptyMap());

    if (cqlType.hasUdt() && !cqlType.getUdt().getFrozen()) {
      invalidMapping(
          "%s: fields that map to UDTs can only be indexed if they are frozen", messagePrefix);
      throw SkipException.INSTANCE;
    }

    return new IndexModel(indexName, indexClass, indexingType, indexOptions);
  }

  private boolean validateTarget(CollectionIndexingType indexTarget) {
    switch (indexTarget) {
      case KEYS:
      case ENTRIES:
        if (!cqlType.hasMap()) {
          invalidMapping(
              "%s: index target %s can only be used for map columns", messagePrefix, indexTarget);
          return false;
        }
        break;
      case VALUES:
        if (!TypeSpecs.isCollection(cqlType) || TypeSpecs.isFrozen(cqlType)) {
          invalidMapping(
              "%s: index target %s can only be used for non-frozen collection columns",
              messagePrefix, indexTarget);
          return false;
        }
        break;
      case FULL:
        if (!TypeSpecs.isCollection(cqlType) || !TypeSpecs.isFrozen(cqlType)) {
          invalidMapping(
              "%s: index target %s can only be used for frozen collection columns",
              messagePrefix, indexTarget);
          return false;
        }
        break;
      default:
        throw new AssertionError("Unsupported target " + indexTarget);
    }
    return true;
  }

  private IndexingType toGrpc(CollectionIndexingType type) {
    switch (type) {
      case KEYS:
        return IndexingType.KEYS;
      case VALUES:
        return IndexingType.VALUES_;
      case ENTRIES:
        return IndexingType.ENTRIES;
      case FULL:
        return IndexingType.FULL;
    }
    throw new IllegalArgumentException("Unexpected type " + type);
  }

  private Optional<Map<String, String>> convertOptions(String rawOptions) {
    try {
      return Optional.of(OPTIONS_SPLITTER.split(rawOptions));
    } catch (IllegalArgumentException e) {
      invalidMapping(
          "%s: invalid format for options. Expected 'option1':'value1','option2','value2'...",
          messagePrefix);
      return Optional.empty();
    }
  }
}
