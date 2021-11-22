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
package io.stargate.grpc;

import stargate.Cql.Relation;
import stargate.Cql.Relation.Target;
import stargate.Cql.Relation.Target.TokenTarget;
import stargate.Cql.Select;
import stargate.Cql.Selector;
import stargate.Cql.Selector.FunctionCallSelector;
import stargate.Cql.Statement;
import stargate.Cql.Term;

/** Helper methods to build common patterns in CQL queries. */
public class CqlHelper {

  public static Statement newStatement(Select.Builder selectBuilder) {
    return Statement.newBuilder().setSelect(selectBuilder).build();
  }

  public static Selector.Builder selectColumn(String columnName) {
    return Selector.newBuilder().setColumnName(columnName);
  }

  /** TODO same for other common one-column methods: min, max, etc. */
  public static Selector.Builder selectWriteTime(String columnName) {
    return columnFunction("writetime", columnName);
  }

  public static Selector.Builder selectToken(String... columnNames) {
    return columnFunction("token", columnNames);
  }

  private static Selector.Builder columnFunction(String functionName, String... columnNames) {
    FunctionCallSelector.Builder functionCallBuilder =
        FunctionCallSelector.newBuilder().setFunctionName(functionName);
    for (String columnName : columnNames) {
      functionCallBuilder.addArguments(selectColumn(columnName));
    }
    return Selector.newBuilder().setFunctionCall(functionCallBuilder);
  }

  public static Relation.Builder whereColumn(String columnName) {
    return Relation.newBuilder().setTarget(Target.newBuilder().setColumnName(columnName));
  }

  public static Relation.Builder whereToken(String... columnNames) {
    TokenTarget.Builder tokenBuilder = TokenTarget.newBuilder();
    for (String columnName : columnNames) {
      tokenBuilder.addColumns(columnName);
    }
    return Relation.newBuilder().setTarget(Target.newBuilder().setToken(tokenBuilder));
  }

  /** TODO same for every Values.of(...) method */
  public static Term.Builder termOf(int v) {
    return Term.newBuilder().setValue(Values.of(v));
  }

  public static Term.Builder termOf(long v) {
    return Term.newBuilder().setValue(Values.of(v));
  }
}
