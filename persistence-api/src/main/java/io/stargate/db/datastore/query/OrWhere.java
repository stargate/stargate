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
package io.stargate.db.datastore.query;

import java.util.stream.Collectors;

import org.immutables.value.Value;

@Value.Immutable(prehash = true)
public abstract class OrWhere<T> implements NWhere<T>
{
    public static OrWhere or(Where... wheres)
    {
        return ImmutableOrWhere.builder().addChildren(wheres).build();
    }

    @Override
    public String toString()
    {
        return children().stream().map(c -> c.toString()).collect(Collectors.joining(" OR ", "(", ")"));
    }
}
