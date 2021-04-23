/*
 * Copyright The Stargate Authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.stargate.web.docsapi.service.query.predicate;

import io.stargate.db.query.Predicate;

import java.util.Optional;

/**
 * Base predicate that can be used in the filter expression.
 */
public interface BasePredicate {

    /**
     * @return Returns raw value of the predicate, as user would specify in the query, f.e. <code>$eq</code>
     */
    String getRawValue();

    /**
     * @return If this predicate can provide database predicate.
     */
    default boolean canProvideDatabasePredicate() {
        return getDatabasePredicate().isPresent();
    }

    /**
     * @return Mirrored database predicate, if one exists.
     */
    Optional<Predicate> getDatabasePredicate();

}
