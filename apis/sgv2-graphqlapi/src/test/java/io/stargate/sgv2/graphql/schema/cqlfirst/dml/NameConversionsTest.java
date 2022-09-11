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
package io.stargate.sgv2.graphql.schema.cqlfirst.dml;

import static io.stargate.sgv2.graphql.schema.cqlfirst.dml.NameConversions.IdentifierType.COLUMN;
import static io.stargate.sgv2.graphql.schema.cqlfirst.dml.NameConversions.IdentifierType.TABLE;
import static io.stargate.sgv2.graphql.schema.cqlfirst.dml.NameConversions.IdentifierType.UDT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import io.stargate.sgv2.graphql.schema.cqlfirst.dml.NameConversions.IdentifierType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class NameConversionsTest {

  @ParameterizedTest
  @MethodSource("names")
  @DisplayName("Should convert CQL identifier to GraphQL and back")
  public void conversionTest(IdentifierType type, String cql, String expectedGraphql) {
    String actualGraphql = NameConversions.toGraphql(cql, type);
    assertThat(actualGraphql).isEqualTo(expectedGraphql);

    // Our implementation only allocates if necessary
    if (actualGraphql.equals(cql)) {
      assertThat(actualGraphql).isSameAs(cql);
    }

    assertThat(NameConversions.toCql(expectedGraphql, type)).isEqualTo(cql);
  }

  public static Arguments[] names() {
    return new Arguments[] {
      // Preserve case, don't remove underscores:
      arguments(TABLE, "foobar", "foobar"),
      arguments(COLUMN, "foo_bar", "foo_bar"),
      arguments(TABLE, "fooBar", "fooBar"),
      arguments(COLUMN, "FooBar", "FooBar"),
      arguments(TABLE, "foo__bar", "foo__bar"),
      // Escape leading digits:
      arguments(TABLE, "123", "x31_23"),
      // Escape leading double underscores:
      arguments(TABLE, "__foobar", "x5f__foobar"),
      arguments(COLUMN, "_foobar", "_foobar"),
      arguments(TABLE, "_", "_"),
      // Escape invalid characters, including unicode:
      arguments(TABLE, "Hello world!", "Hellox20_worldx21_"),
      arguments(COLUMN, "\uD83D\uDD77", "x1f577_"),
      arguments(TABLE, "ὗ7", "x1f57_7"),
      // Reserved keywords (table only):
      arguments(TABLE, "Mutation", "Mutatiox6e_"),
      arguments(TABLE, "MutationConsistency", "MutationConsistencx79_"),
      arguments(TABLE, "MutationOptions", "MutationOptionx73_"),
      arguments(TABLE, "Query", "Querx79_"),
      arguments(TABLE, "QueryConsistency", "QueryConsistencx79_"),
      arguments(TABLE, "QueryOptions", "QueryOptionx73_"),
      arguments(TABLE, "SerialConsistency", "SerialConsistencx79_"),
      arguments(TABLE, "Boolean", "Booleax6e_"),
      arguments(TABLE, "Float", "Floax74_"),
      arguments(TABLE, "Int", "Inx74_"),
      arguments(TABLE, "String", "Strinx67_"),
      arguments(TABLE, "Uuid", "Uuix64_"),
      arguments(TABLE, "TimeUuid", "TimeUuix64_"),
      arguments(TABLE, "Inet", "Inex74_"),
      arguments(TABLE, "Date", "Datx65_"),
      arguments(TABLE, "Duration", "Duratiox6e_"),
      arguments(TABLE, "BigInt", "BigInx74_"),
      arguments(TABLE, "Counter", "Countex72_"),
      arguments(TABLE, "Ascii", "Ascix69_"),
      arguments(TABLE, "Decimal", "Decimax6c_"),
      arguments(TABLE, "Varint", "Varinx74_"),
      arguments(TABLE, "Float32", "Float32"),
      arguments(TABLE, "Blob", "Blox62_"),
      arguments(TABLE, "SmallInt", "SmallInx74_"),
      arguments(TABLE, "TinyInt", "TinyInx74_"),
      arguments(TABLE, "Timestamp", "Timestamx70_"),
      arguments(TABLE, "Time", "Timx65_"),
      // Reserved keywords don't apply to UDT or columns:
      arguments(COLUMN, "Mutation", "Mutation"),
      arguments(UDT, "Mutation", "MutationUdt"),
      // Reserved prefixes (table or UDT):
      arguments(TABLE, "EntryFoo", "Entrx79_Foo"),
      arguments(TABLE, "TupleFoo", "Tuplx65_Foo"),
      arguments(UDT, "EntryFoo", "Entrx79_FooUdt"),
      arguments(UDT, "TupleFoo", "Tuplx65_FooUdt"),
      // Reserved suffixes (table only):
      arguments(TABLE, "FooFilterInput", "FooFilterInpux74_"),
      arguments(TABLE, "FooInput", "FooInpux74_"),
      arguments(TABLE, "FooResult", "FooResulx74_"),
      arguments(TABLE, "FooMutationResult", "FooMutationResulx74_"),
      arguments(TABLE, "FooOrder", "FooOrdex72_"),
      arguments(TABLE, "FooUdt", "FooUdx74_"),
      // Reserved suffixes don't apply to UDTs or columns:
      arguments(COLUMN, "FooInput", "FooInput"),
      arguments(COLUMN, "FooUdt", "FooUdt"),
      arguments(UDT, "FooInput", "FooInputUdt"),
      arguments(UDT, "FooUdt", "FooUdtUdt"),
    };
  }
}
