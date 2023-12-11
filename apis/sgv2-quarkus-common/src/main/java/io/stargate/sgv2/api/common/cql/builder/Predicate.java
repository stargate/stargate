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

public enum Predicate {
  EQ("="),
  NEQ("!="),
  LT("<"),
  GT(">"),
  LTE("<="),
  GTE(">="),
  IN("IN"),
  CONTAINS("CONTAINS"),
  NOT_CONTAINS("NOT CONTAINS"),
  CONTAINS_KEY("CONTAINS KEY"),
  ;

  private final String cql;

  Predicate(String cql) {
    this.cql = cql;
  }

  @Override
  public String toString() {
    return cql;
  }
}
