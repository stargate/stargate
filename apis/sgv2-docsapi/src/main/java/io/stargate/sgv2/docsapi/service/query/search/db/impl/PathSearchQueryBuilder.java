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
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.stargate.sgv2.docsapi.service.query.search.db.impl;

import static io.stargate.sgv2.docsapi.config.constants.Constants.GLOB_ARRAY_VALUE;
import static io.stargate.sgv2.docsapi.config.constants.Constants.GLOB_VALUE;

import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.api.common.cql.builder.BuiltCondition;
import io.stargate.sgv2.api.common.cql.builder.Predicate;
import io.stargate.sgv2.api.common.cql.builder.Term;
import io.stargate.sgv2.docsapi.api.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.api.properties.document.DocumentTableProperties;
import io.stargate.sgv2.docsapi.service.query.search.db.AbstractSearchQueryBuilder;
import io.stargate.sgv2.docsapi.service.util.DocsApiUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;

/**
 * The search query builder that creates all needed predicates for a path represented as a list of
 * string.
 */
public class PathSearchQueryBuilder extends AbstractSearchQueryBuilder {

  private final List<String> path;
  private Pair<List<BuiltCondition>, List<QueryOuterClass.Value>> conditionsAndValues;

  /**
   * @param path Path to match.
   */
  public PathSearchQueryBuilder(DocumentProperties documentProperties, List<String> path) {
    super(documentProperties);
    this.path = path;
  }

  /**
   * Resolves the pair that contains the predicates and values that are known at the creation time.
   * The conditions must be created with the marker.
   *
   * <p>Subclasses can override, but always must include the result of the super call.
   *
   * @return Pair of conditions to value list. Lists must be same size.
   */
  protected Pair<List<BuiltCondition>, List<QueryOuterClass.Value>> resolve() {
    DocumentTableProperties tableProps = documentProperties.tableProperties();

    List<BuiltCondition> predicates = new ArrayList<>();
    List<QueryOuterClass.Value> values = new ArrayList<>();

    // copied from the DocumentService
    for (int i = 0; i < path.size(); i++) {
      String next = path.get(i);
      String[] pathSegmentSplit = next.split(DocsApiUtils.COMMA_PATTERN.pattern());
      if (pathSegmentSplit.length == 1) {
        String pathSegment = pathSegmentSplit[0];
        if (pathSegment.equals(GLOB_VALUE) || pathSegment.equals(GLOB_ARRAY_VALUE)) {
          predicates.add(
              BuiltCondition.of(tableProps.pathColumnName(i), Predicate.GT, Term.marker()));
          values.add(Values.of(""));
        } else {
          predicates.add(
              BuiltCondition.of(tableProps.pathColumnName(i), Predicate.EQ, Term.marker()));
          values.add(Values.of(DocsApiUtils.convertEscapedCharacters(pathSegment)));
        }
      } else {
        List<QueryOuterClass.Value> segmentValues =
            Arrays.stream(pathSegmentSplit)
                .map(DocsApiUtils::convertEscapedCharacters)
                .map(Values::of)
                .toList();

        predicates.add(
            BuiltCondition.of(tableProps.pathColumnName(i), Predicate.IN, Term.marker()));
        values.add(Values.of(segmentValues));
      }
    }

    return Pair.of(predicates, values);
  }

  @Override
  protected boolean allowFiltering() {
    return !path.isEmpty();
  }

  /** {@inheritDoc} */
  @Override
  protected List<BuiltCondition> getBindPredicates() {
    return Collections.emptyList();
  }

  /** {@inheritDoc} */
  @Override
  protected final List<BuiltCondition> getPredicates() {
    ensureResolved();
    return conditionsAndValues.getLeft();
  }

  /** {@inheritDoc} */
  @Override
  protected final List<QueryOuterClass.Value> getValues() {
    ensureResolved();
    return conditionsAndValues.getRight();
  }

  private void ensureResolved() {
    if (null == conditionsAndValues) {
      conditionsAndValues = resolve();

      // ensure size
      int conditions = conditionsAndValues.getLeft().size();
      int values = conditionsAndValues.getRight().size();
      if (conditions != values) {
        throw new IllegalStateException(
            "Size of the conditions (%d) does not match the size of the bind values (%d)."
                .formatted(conditions, values));
      }
    }
  }
}
