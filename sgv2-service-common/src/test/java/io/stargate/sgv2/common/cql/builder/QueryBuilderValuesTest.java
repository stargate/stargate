/*
 * Copyright DataStax, Inc. and/or The Stargate Authors
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
package io.stargate.sgv2.common.cql.builder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

import io.stargate.grpc.Values;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.QueryOuterClass.Query;
import io.stargate.proto.QueryOuterClass.Value;
import io.stargate.sgv2.common.cql.builder.BuiltCondition.LHS;
import org.junit.jupiter.api.Test;

public class QueryBuilderValuesTest {

  public static final Value INT_VALUE1 = Values.of(1);
  public static final Value INT_VALUE2 = Values.of(10);
  public static final Value TEXT_VALUE = Values.of("a");

  @Test
  public void shouldBindDirectInsertValues() {
    Query query =
        new QueryBuilder()
            .insertInto("ks", "tbl")
            .value("c1", INT_VALUE1)
            .value("c2", TEXT_VALUE)
            .value("c3", 3)
            .build();

    assertThat(query.getCql())
        .isEqualTo("INSERT INTO ks.tbl (c1, c2, c3) VALUES (:\"c1\", :\"c2\", 3)");

    QueryOuterClass.Values values = query.getValues();
    assertThat(values.getValuesCount()).isEqualTo(2);
    assertValue(values, "c1", INT_VALUE1);
    assertValue(values, "c2", TEXT_VALUE);
  }

  @Test
  public void shouldBindValueModifiers() {
    Query query =
        new QueryBuilder()
            .update("ks", "tbl")
            .value(ValueModifier.set("c1", INT_VALUE1))
            .value(
                ValueModifier.of(
                    ValueModifier.Target.mapValue("c2", Term.of(TEXT_VALUE)),
                    ValueModifier.Operation.APPEND,
                    Term.of(1)))
            .where("k", Predicate.EQ, 1)
            .build();

    assertThat(query.getCql())
        .isEqualTo("UPDATE ks.tbl SET c1 = :\"c1\", c2[:\"c2\"] += 1 WHERE k = 1");

    QueryOuterClass.Values values = query.getValues();
    assertThat(values.getValuesCount()).isEqualTo(2);
    assertValue(values, "c1", INT_VALUE1);
    assertValue(values, "c2", TEXT_VALUE);
  }

  @Test
  public void shouldBindDirectWhereValues() {
    Query query =
        new QueryBuilder()
            .select()
            .from("ks", "tbl")
            .where("c1", Predicate.EQ, INT_VALUE1)
            .where("c2", Predicate.EQ, TEXT_VALUE)
            .where("c3", Predicate.EQ, 3)
            .build();

    assertThat(query.getCql())
        .isEqualTo("SELECT * FROM ks.tbl WHERE c1 = :\"c1\" AND c2 = :\"c2\" AND c3 = 3");

    QueryOuterClass.Values values = query.getValues();
    assertThat(values.getValuesCount()).isEqualTo(2);
    assertValue(values, "c1", INT_VALUE1);
    assertValue(values, "c2", TEXT_VALUE);
  }

  @Test
  public void shouldBindBuiltConditionsInWhere() {
    Query query =
        new QueryBuilder()
            .select()
            .from("ks", "tbl")
            .where(BuiltCondition.of("c1", Predicate.EQ, INT_VALUE1))
            .where(BuiltCondition.of(LHS.mapAccess("c2", TEXT_VALUE), Predicate.GT, Term.of(1)))
            .build();

    assertThat(query.getCql())
        .isEqualTo("SELECT * FROM ks.tbl WHERE c1 = :\"c1\" AND c2[:\"c2\"] > 1");

    QueryOuterClass.Values values = query.getValues();
    assertThat(values.getValuesCount()).isEqualTo(2);
    assertValue(values, "c1", INT_VALUE1);
    assertValue(values, "c2", TEXT_VALUE);
  }

  @Test
  public void shouldBindDirectIfValues() {
    Query query =
        new QueryBuilder()
            .delete()
            .from("ks", "tbl")
            .where("k", Predicate.EQ, 1)
            .ifs("c1", Predicate.EQ, INT_VALUE1)
            .ifs("c2", Predicate.EQ, TEXT_VALUE)
            .ifs("c3", Predicate.EQ, 3)
            .build();

    assertThat(query.getCql())
        .isEqualTo("DELETE FROM ks.tbl WHERE k = 1 IF c1 = :\"c1\" AND c2 = :\"c2\" AND c3 = 3");

    QueryOuterClass.Values values = query.getValues();
    assertThat(values.getValuesCount()).isEqualTo(2);
    assertValue(values, "c1", INT_VALUE1);
    assertValue(values, "c2", TEXT_VALUE);
  }

  @Test
  public void shouldBindBuiltConditionsInIfs() {
    Query query =
        new QueryBuilder()
            .delete()
            .from("ks", "tbl")
            .where("k", Predicate.EQ, 1)
            .ifs(BuiltCondition.of("c1", Predicate.EQ, INT_VALUE1))
            .ifs(BuiltCondition.of(LHS.mapAccess("c2", TEXT_VALUE), Predicate.GT, Term.of(1)))
            .build();

    assertThat(query.getCql())
        .isEqualTo("DELETE FROM ks.tbl WHERE k = 1 IF c1 = :\"c1\" AND c2[:\"c2\"] > 1");

    QueryOuterClass.Values values = query.getValues();
    assertThat(values.getValuesCount()).isEqualTo(2);
    assertValue(values, "c1", INT_VALUE1);
    assertValue(values, "c2", TEXT_VALUE);
  }

  @Test
  public void shouldGenerateUniqueMarkerNames() {
    Query query =
        new QueryBuilder()
            .delete()
            .from("ks", "tbl")
            .where("k", Predicate.GT, INT_VALUE1)
            .where("k", Predicate.LTE, INT_VALUE2)
            .build();

    assertThat(query.getCql()).isEqualTo("DELETE FROM ks.tbl WHERE k > :\"k\" AND k <= :\"k2\"");

    QueryOuterClass.Values values = query.getValues();
    assertThat(values.getValuesCount()).isEqualTo(2);
    assertValue(values, "k", INT_VALUE1);
    assertValue(values, "k2", INT_VALUE2);
  }

  @Test
  public void shouldNotAllowMixedMarkers() {
    assertThatThrownBy(
            () ->
                new QueryBuilder()
                    .select()
                    .from("ks", "tbl")
                    .where("c1", Predicate.EQ, INT_VALUE1)
                    .where("c2", Predicate.EQ, Term.marker()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Can't use anonymous and named markers in the same query");
    assertThatThrownBy(
            () ->
                new QueryBuilder()
                    .select()
                    .from("ks", "tbl")
                    .where("c1", Predicate.EQ, Term.marker())
                    .where("c2", Predicate.EQ, INT_VALUE1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Can't use anonymous and named markers in the same query");
  }

  @Test
  public void shouldAllowAnonymousMarkersWhenNoValues() {
    Query query =
        new QueryBuilder()
            .select()
            .from("ks", "tbl")
            .where("c1", Predicate.EQ, Term.marker())
            .build();

    assertThat(query.getCql()).isEqualTo("SELECT * FROM ks.tbl WHERE c1 = ?");
  }

  private void assertValue(QueryOuterClass.Values values, String name, Value value) {
    for (int i = 0; i < values.getValueNamesCount(); i++) {
      if (name.equals(values.getValueNames(i))) {
        assertThat(values.getValues(i)).isEqualTo(value);
        return;
      }
    }
    fail("Did not find value name %s in %s", name, values.getValueNamesList());
  }
}
