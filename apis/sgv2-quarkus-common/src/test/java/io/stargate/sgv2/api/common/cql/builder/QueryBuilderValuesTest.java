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
package io.stargate.sgv2.api.common.cql.builder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.bpodgursky.jbool_expressions.And;
import com.bpodgursky.jbool_expressions.Expression;
import com.bpodgursky.jbool_expressions.Or;
import com.bpodgursky.jbool_expressions.Variable;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.QueryOuterClass.Query;
import io.stargate.bridge.proto.QueryOuterClass.Value;
import io.stargate.sgv2.api.common.cql.builder.BuiltCondition.LHS;
import java.util.List;
import org.junit.jupiter.api.Test;

public class QueryBuilderValuesTest {

  public static final Value INT_VALUE1 = Values.of(1);
  public static final Value INT_VALUE2 = Values.of(10);
  public static final Value TEXT_VALUE = Values.of("a");

  public static final Value TEST_NAME_VALUE = Values.of("tim");
  public static final Value TEST_AGE_VALUE = Values.of(25);
  public static final Value TEST_GENDER_VALUE = Values.of("male");
  public static final Value TEST_KEY_VALUE = Values.of("(1,'1')");


  @Test
  public void shouldBindDirectInsertValues() {
    Query query =
        new QueryBuilder()
            .insertInto("ks", "tbl")
            .value("c1", INT_VALUE1)
            .value("c2", TEXT_VALUE)
            .build();

    assertThat(query.getCql()).isEqualTo("INSERT INTO ks.tbl (c1, c2) VALUES (?, ?)");
    assertThat(query.getValues().getValuesList()).containsExactly(INT_VALUE1, TEXT_VALUE);
  }

  @Test
  public void shouldBindSimilarityCosine() {
    Value vectorValue = Values.of(List.of(Values.of(1.0f)));
    Query query =
        new QueryBuilder()
            .select()
            .column("a", "b", "c")
            .similarityCosine("vector_column", vectorValue)
            .from("ks", "tbl")
            .limit(1)
            .vsearch("vector_column")
            .build();

    assertThat(query.getCql())
        .isEqualTo(
            "SELECT a, b, c, SIMILARITY_COSINE(vector_column, ?) FROM ks.tbl ORDER BY vector_column ANN OF ? LIMIT 1");
    assertThat(query.getValues().getValuesList()).containsExactly(vectorValue);
  }

  @Test
  public void shouldBindSimilarityDotProduct() {
    Value vectorValue = Values.of(List.of(Values.of(1.0f)));
    Query query =
        new QueryBuilder()
            .select()
            .column("a", "b", "c")
            .similarityDotProduct("vector_column", vectorValue)
            .from("ks", "tbl")
            .limit(1)
            .vsearch("vector_column")
            .build();

    assertThat(query.getCql())
        .isEqualTo(
            "SELECT a, b, c, SIMILARITY_DOT_PRODUCT(vector_column, ?) FROM ks.tbl ORDER BY vector_column ANN OF ? LIMIT 1");
    assertThat(query.getValues().getValuesList()).containsExactly(vectorValue);
  }

  @Test
  public void shouldBindSimilarityEuclidean() {
    Value vectorValue = Values.of(List.of(Values.of(1.0f)));
    Query query =
        new QueryBuilder()
            .select()
            .column("a", "b", "c")
            .similarityEuclidean("vector_column", vectorValue)
            .from("ks", "tbl")
            .limit(1)
            .vsearch("vector_column")
            .build();

    assertThat(query.getCql())
        .isEqualTo(
            "SELECT a, b, c, SIMILARITY_EUCLIDEAN(vector_column, ?) FROM ks.tbl ORDER BY vector_column ANN OF ? LIMIT 1");
    assertThat(query.getValues().getValuesList()).containsExactly(vectorValue);
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
                    INT_VALUE2))
            .where("k", Predicate.EQ, INT_VALUE1)
            .build();

    assertThat(query.getCql()).isEqualTo("UPDATE ks.tbl SET c1 = ?, c2[?] += ? WHERE k = ?");
    assertThat(query.getValues().getValuesList())
        .containsExactly(INT_VALUE1, TEXT_VALUE, INT_VALUE2, INT_VALUE1);
  }

  @Test
  public void shouldBindDirectWhereValues() {
    Query query =
        new QueryBuilder()
            .select()
            .from("ks", "tbl")
            .where("c1", Predicate.EQ, INT_VALUE1)
            .where("c2", Predicate.EQ, TEXT_VALUE)
            .build();

    assertThat(query.getCql()).isEqualTo("SELECT * FROM ks.tbl WHERE c1 = ? AND c2 = ?");
    assertThat(query.getValues().getValuesList()).containsExactly(INT_VALUE1, TEXT_VALUE);
  }

  @Test
  public void shouldBindBuiltConditionsInWhere() {
    Query query =
        new QueryBuilder()
            .select()
            .from("ks", "tbl")
            .where(BuiltCondition.of("c1", Predicate.EQ, INT_VALUE1))
            .where(BuiltCondition.of(LHS.mapAccess("c2", TEXT_VALUE), Predicate.GT, INT_VALUE2))
            .build();

    assertThat(query.getCql()).isEqualTo("SELECT * FROM ks.tbl WHERE c1 = ? AND c2[?] > ?");
    assertThat(query.getValues().getValuesList())
        .containsExactly(INT_VALUE1, TEXT_VALUE, INT_VALUE2);
  }

  @Test
  public void shouldBindDirectIfValues() {
    Query query =
        new QueryBuilder()
            .delete()
            .from("ks", "tbl")
            .where("k", Predicate.EQ, INT_VALUE1)
            .ifs("c1", Predicate.EQ, INT_VALUE2)
            .ifs("c2", Predicate.EQ, TEXT_VALUE)
            .build();

    assertThat(query.getCql()).isEqualTo("DELETE FROM ks.tbl WHERE k = ? IF c1 = ? AND c2 = ?");
    assertThat(query.getValues().getValuesList())
        .containsExactly(INT_VALUE1, INT_VALUE2, TEXT_VALUE);
  }

  @Test
  public void shouldBindBuiltConditionsInIfs() {
    Query query =
        new QueryBuilder()
            .delete()
            .from("ks", "tbl")
            .where("k", Predicate.EQ, INT_VALUE1)
            .ifs(BuiltCondition.of("c1", Predicate.EQ, INT_VALUE2))
            .ifs(BuiltCondition.of(LHS.mapAccess("c2", TEXT_VALUE), Predicate.GT, INT_VALUE1))
            .build();

    assertThat(query.getCql()).isEqualTo("DELETE FROM ks.tbl WHERE k = ? IF c1 = ? AND c2[?] > ?");
    assertThat(query.getValues().getValuesList())
        .containsExactly(INT_VALUE1, INT_VALUE2, TEXT_VALUE, INT_VALUE1);
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

    assertThat(query.getCql()).isEqualTo("DELETE FROM ks.tbl WHERE k > ? AND k <= ?");
    assertThat(query.getValues().getValuesList()).containsExactly(INT_VALUE1, INT_VALUE2);
  }

  @Test
  public void shouldBindValuesInQueryOrder() {
    // This is a bit contrived, client code should not explicitly depend on generated classes like
    // this:
    QueryBuilder.QueryBuilder__23 select = new QueryBuilder().select().from("ks", "tbl");
    select.limit(INT_VALUE2);
    select.where(BuiltCondition.of("c1", Predicate.EQ, INT_VALUE1));
    Query query = select.build();

    assertThat(query.getCql()).isEqualTo("SELECT * FROM ks.tbl WHERE c1 = ? LIMIT ?");
    assertThat(query.getValues().getValuesList()).containsExactly(INT_VALUE1, INT_VALUE2);
  }

  @Test
  public void shouldFailIfMixingValuesAndExplicitMarkers() {
    assertThatThrownBy(
            () ->
                new QueryBuilder()
                    .select()
                    .from("tbl")
                    .where("a", Predicate.EQ, INT_VALUE1)
                    .where("b", Predicate.EQ, Term.marker()))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void shouldAllowCQLStatement() {
    Query query = new QueryBuilder().cql("SELECT * from system.local").build();
    assertThat(query.getCql()).isEqualTo("SELECT * from system.local");
  }

  @Test
  public void expressionTest() {
    BuiltCondition condition = BuiltCondition.of("name", Predicate.EQ, TEST_NAME_VALUE);
    BuiltCondition age = BuiltCondition.of("age", Predicate.EQ, TEST_AGE_VALUE);
    BuiltCondition gender = BuiltCondition.of("gender", Predicate.CONTAINS, TEST_GENDER_VALUE);

    Expression<BuiltCondition> expr =
            And.of(Variable.of(condition), Or.of(Variable.of(age), Variable.of(gender)));
    Query query =
            new QueryBuilder().select().from("testKS", "testCollection").where(expr).limit(1).build();
    System.out.println("ooo " + query.getCql());
    System.out.println("ooo " + query.getValues());

    assertThat(query.getCql()).isEqualTo("SELECT * FROM \"testKS\".\"testCollection\" WHERE  (  ( gender CONTAINS ? OR age = ? )  AND name = ? )  LIMIT 1");
    assertThat(query.getValues().getValuesList())
            .contains(TEST_NAME_VALUE, TEST_AGE_VALUE,TEST_GENDER_VALUE);
  }

  @Test
  public void expressionQueryBuilderTest() {
    BuiltCondition name = BuiltCondition.of("name", Predicate.EQ, TEST_NAME_VALUE);
    BuiltCondition age = BuiltCondition.of("age", Predicate.EQ, TEST_AGE_VALUE);
    BuiltCondition gender = BuiltCondition.of("gender", Predicate.CONTAINS, TEST_GENDER_VALUE);

    Expression<BuiltCondition> expr =
            And.of(Variable.of(name), Or.of(Variable.of(age), Variable.of(gender)));
    Query query =
            new QueryBuilder().select().from("testKS", "testCollection").where(expr).limit(1).build();
    System.out.println("ooo " + query.getCql());
    System.out.println("ooo " + query.getValues());

    assertThat(query.getCql()).isEqualTo("SELECT * FROM \"testKS\".\"testCollection\" WHERE  (  ( gender CONTAINS ? OR age = ? )  AND name = ? )  LIMIT 1");
    assertThat(query.getValues().getValuesList())
            .contains(TEST_NAME_VALUE, TEST_AGE_VALUE,TEST_GENDER_VALUE);
  }


  @Test
  public void expressionQueryBuilderKeyTest() {
    BuiltCondition key = BuiltCondition.of("key", Predicate.EQ, TEST_KEY_VALUE);
    BuiltCondition name = BuiltCondition.of("name", Predicate.CONTAINS, TEST_NAME_VALUE);
    BuiltCondition gender = BuiltCondition.of("gender", Predicate.CONTAINS, TEST_GENDER_VALUE);

    Expression<BuiltCondition> expr =
            And.of(Variable.of(key), Or.of(Variable.of(name), Variable.of(gender)));
    Query query =
            new QueryBuilder().select().from("testKS", "testCollection").where(expr).build();

    assertThat(query.getCql()).isEqualTo("SELECT * FROM \"testKS\".\"testCollection\" WHERE  ( key = ? AND  ( gender CONTAINS ? OR name CONTAINS ? )  ) ");
    assertThat(query.getValues().getValuesList())
            .contains(TEST_KEY_VALUE,TEST_NAME_VALUE,TEST_GENDER_VALUE);
  }



}
