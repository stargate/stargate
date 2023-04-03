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

import com.datastax.oss.driver.shaded.guava.common.base.CharMatcher;
import com.datastax.oss.driver.shaded.guava.common.base.Splitter;
import graphql.language.Directive;
import io.stargate.db.schema.CollectionIndexingType;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.ImmutableCollectionIndexingType;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

class IndexModelBuilder extends ModelBuilderBase<IndexModel> {

  private static final Splitter.MapSplitter OPTIONS_SPLITTER =
      Splitter.on(',').withKeyValueSeparator(Splitter.on(':').trimResults(CharMatcher.anyOf("' ")));
  private static final ImmutableCollectionIndexingType NO_INDEXING_TYPE =
      ImmutableCollectionIndexingType.builder().build();
  private static final ImmutableCollectionIndexingType VALUES_INDEXING_TYPE =
      ImmutableCollectionIndexingType.builder().indexValues(true).build();

  private final Directive cqlIndexDirective;
  private final String parentCqlName;
  private final String columnCqlName;
  private final Column.ColumnType cqlType;
  private final String messagePrefix;

  IndexModelBuilder(
      Directive cqlIndexDirective,
      String parentCqlName,
      String columnCqlName,
      Column.ColumnType cqlType,
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
    if (!indexClass.isPresent() && !context.getPersistence().supportsSecondaryIndex()) {
      if (!context.getPersistence().supportsSAI()) {
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

    CollectionIndexingType indexingType =
        DirectiveHelper.getEnumArgument(
                cqlIndexDirective, CqlDirectives.INDEX_TARGET, IndexTarget.class, context)
            .filter(this::validateTarget)
            .map(IndexTarget::toIndexingType)
            .orElse(cqlType.isCollection() ? VALUES_INDEXING_TYPE : NO_INDEXING_TYPE);
    Map<String, String> indexOptions =
        DirectiveHelper.getStringArgument(cqlIndexDirective, CqlDirectives.INDEX_OPTIONS, context)
            .flatMap(this::convertOptions)
            .orElse(Collections.emptyMap());

    if (cqlType.isUserDefined() && !cqlType.isFrozen()) {
      invalidMapping(
          "%s: fields that map to UDTs can only be indexed if they are frozen", messagePrefix);
      throw SkipException.INSTANCE;
    }

    return new IndexModel(indexName, indexClass, indexingType, indexOptions);
  }

  private boolean validateTarget(IndexTarget indexTarget) {
    switch (indexTarget) {
      case KEYS:
      case ENTRIES:
        if (!cqlType.isMap()) {
          invalidMapping(
              "%s: index target %s can only be used for map columns", messagePrefix, indexTarget);
          return false;
        }
        break;
      case VALUES:
        if (!cqlType.isCollection() || cqlType.isFrozen()) {
          invalidMapping(
              "%s: index target %s can only be used for non-frozen collection columns",
              messagePrefix, indexTarget);
          return false;
        }
        break;
      case FULL:
        if (!cqlType.isCollection() || !cqlType.isFrozen()) {
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
