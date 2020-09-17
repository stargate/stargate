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
package io.stargate.db.datastore.schema;

import javax.annotation.Nullable;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;

import org.immutables.value.Value;

import io.stargate.db.datastore.query.ColumnOrder;
import io.stargate.db.datastore.query.WhereCondition;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;

@Value.Immutable(prehash = true)
public abstract class SecondaryIndex implements Index, QualifiedSchemaEntity
{
    private static final long serialVersionUID = 424886903165529554L;

    private static final Set<WhereCondition.Predicate> ALLOWED_PREDICATES = ImmutableSet.of(WhereCondition.Predicate.Eq, WhereCondition.Predicate.Contains, WhereCondition.Predicate.ContainsKey,
            WhereCondition.Predicate.ContainsValue, WhereCondition.Predicate.EntryEq);

    @Nullable
    public abstract Column column();

    public abstract CollectionIndexingType indexingType();

    public static SecondaryIndex create(String keyspace, String name, Column column)
    {
        return ImmutableSecondaryIndex.builder().keyspace(keyspace).name(name).column(column)
                .indexingType(ImmutableCollectionIndexingType.builder().build())
                .build();
    }

    public static SecondaryIndex create(String keyspace, String name, Column column,
                                        CollectionIndexingType indexingType)
    {
        return ImmutableSecondaryIndex.builder().keyspace(keyspace).name(name).column(column).indexingType(indexingType)
                .build();
    }

    public static SecondaryIndex reference(String name)
    {
        return ImmutableSecondaryIndex.builder().keyspace("ignored-maybe").name(name)
                .indexingType(ImmutableCollectionIndexingType.builder().build()).build();
    }

    @Override
    public boolean supports(List<Column> select, List<WhereCondition<?>> conditions, List<ColumnOrder> orders,
                            OptionalLong limit)
    {
        if (!orders.isEmpty())
        {
            // Secondary index does not support orders
            return false;
        }

        if (conditions.isEmpty())
        {
            // Secondary index does not support full scan
            return false;
        }

        if (conditions.size() > 1 && !eqUsedOnAllNonMatchingPKColumns(conditions))
        {
            // multiple conditions are not supported
            return false;
        }

        return conditions.stream()
                .anyMatch(w -> matchesColumn(w) && predicateAllowed(w) && predicateSupportedByIndexingType(w));
    }

    private boolean eqUsedOnAllNonMatchingPKColumns(List<WhereCondition<?>> conditions)
    {
        return conditions.stream()
                .filter(w -> !matchesColumn(w))
                .allMatch(w -> w.column().isPrimaryKeyComponent() && WhereCondition.Predicate.Eq.equals(w.predicate()));
    }

    private boolean matchesColumn(WhereCondition<?> w)
    {
        return w.column().reference().equals(column().reference());
    }

    @Override
    public int priority()
    {
        return 2;
    }

    private boolean predicateAllowed(WhereCondition<?> w)
    {
        WhereCondition.Predicate predicate = w.predicate();
        if (!ALLOWED_PREDICATES.contains(predicate))
        {
            return false;
        }

        if (WhereCondition.Predicate.Eq.equals(predicate))
        {
            return column().isPrimaryKeyComponent() || column().isFrozenCollection() || matchesColumn(w);
        }

        if (WhereCondition.Predicate.ContainsKey.equals(predicate) || WhereCondition.Predicate.EntryEq.equals(predicate) || WhereCondition.Predicate.ContainsValue.equals(predicate))
        {
            return column().ofTypeMap() && !column().isFrozenCollection();
        }

        if (WhereCondition.Predicate.Contains.equals(predicate))
        {
            return column().ofTypeListOrSet() && !column().isFrozenCollection();
        }

        return true;
    }

    private boolean predicateSupportedByIndexingType(WhereCondition<?> w)
    {
        WhereCondition.Predicate predicate = w.predicate();
        if (!column().isCollection())
        {
            // nothing to check if it's not a CQL collection
            return true;
        }

        if (indexingType().indexFull())
        {
            return true;
        }

        if (indexingType().indexKeys())
        {
            return WhereCondition.Predicate.ContainsKey.equals(predicate);
        }

        if (indexingType().indexValues())
        {
            return WhereCondition.Predicate.ContainsValue.equals(predicate) || WhereCondition.Predicate.Contains.equals(predicate);
        }

        if (indexingType().indexEntries())
        {
            return WhereCondition.Predicate.EntryEq.equals(predicate);
        }
        return false;
    }

    @Override
    public String indexTypeName()
    {
        return "Secondary index";
    }
}
