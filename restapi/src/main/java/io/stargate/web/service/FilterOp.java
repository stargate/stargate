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
package io.stargate.web.service;

import io.stargate.db.query.Predicate;

public enum FilterOp {
  $EQ("==", Predicate.EQ, "$eq"),
  $LT("<", Predicate.LT, "$lt"),
  $LTE("<=", Predicate.LTE, "$lte"),
  $GT(">", Predicate.GT, "$gt"),
  $GTE(">=", Predicate.GTE, "$gte"),
  $EXISTS("==", Predicate.EQ, "$exists"),
  $IN("IN", Predicate.IN, "$in"),
  $CONTAINS("CONTAINS", Predicate.CONTAINS, "$contains"),
  $CONTAINSKEY("CONTAINS KEY", Predicate.CONTAINS_KEY, "$containsKey"),
  $CONTAINSENTRY("==", Predicate.EQ, "$containsEntry"),
  ;
  // NE("!=", WhereCondition.Predicate.Neq) CQL 3.4.5 doesn't support <>
  // NIN(...) CQL 3.4.5 doesn't support NOT IN

  public final String cqlOp;
  public final Predicate predicate;
  public final String rawValue;

  FilterOp(String name, Predicate predicate, String rawValue) {
    this.cqlOp = name;
    this.predicate = predicate;
    this.rawValue = rawValue;
  }
}
