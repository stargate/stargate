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

package io.stargate.web.docsapi.service.query.filter.operation;

/**
 * Special type of predicate that can test a generic value against all database values.
 *
 * @param <V> Type of a value used in the predicate.
 */
public interface CustomValueFilterOperation<V> extends StringValueFilterOperation<V>, DoubleValueFilterOperation<V>, BooleanValueFilterOperation<V> {

    /**
     * @return Defines if all database values much match the predicate for the predicate to test true.
     */
    boolean isMatchAll();

}
