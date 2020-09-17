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

import io.stargate.db.datastore.query.WhereCondition;

public enum FilterOp {
    $EQ("==", WhereCondition.Predicate.Eq, "$eq"),
    $LT("<", WhereCondition.Predicate.Lt, "$lt"),
    $LTE("<=", WhereCondition.Predicate.Lte, "$lte"),
    $GT(">", WhereCondition.Predicate.Gt, "$gt"),
    $GTE(">=", WhereCondition.Predicate.Gte, "$gte"),
    $EXISTS("==", WhereCondition.Predicate.Eq, "$exists"),
    $IN("IN", WhereCondition.Predicate.In, "$in");
    // NE("!=", WhereCondition.Predicate.Neq) CQL 3.4.5 doesn't support <>
    // NIN(...) CQL 3.4.5 doesn't support NOT IN

    public final String cqlOp;
    public final WhereCondition.Predicate predicate;
    public final String rawValue;
    FilterOp(String name, WhereCondition.Predicate predicate, String rawValue) {
        this.cqlOp = name;
        this.predicate = predicate;
        this.rawValue = rawValue;
    }
}
