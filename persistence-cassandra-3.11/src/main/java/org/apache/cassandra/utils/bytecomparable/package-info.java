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
/**
 * Content of this package was copied from the Cassandra trunk that's set to be released in the
 * version 4.2. We need this temporary, until 3.11 EOL, in order to do basic comparable bytes
 * generation in the {@link io.stargate.db.cassandra.impl.RowDecoratorImpl}.
 *
 * <p>Note that classes in the package might be different from the original ones. Mainly in order to
 * have only needed functionality.
 *
 * @see
 *     https://github.com/apache/cassandra/tree/trunk/src/java/org/apache/cassandra/utils/bytecomparable
 */
package org.apache.cassandra.utils.bytecomparable;
