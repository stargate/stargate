/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package io.stargate.db.datastore.query;

import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.immutables.value.Value;

import io.stargate.db.datastore.schema.Column;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;

import static io.stargate.db.datastore.query.WhereCondition.Predicate.Contains;
import static io.stargate.db.datastore.query.WhereCondition.Predicate.ContainsKey;
import static io.stargate.db.datastore.query.WhereCondition.Predicate.ContainsValue;
import static io.stargate.db.datastore.query.WhereCondition.Predicate.EntryEq;

@Value.Immutable(prehash = true)
public abstract class WhereCondition<T> implements Parameter<T>, Where<T>
{
    public static final String PATH_DELIMITER = ".";

    private static final Set<Predicate> CQL_MAP_PREDICATES = ImmutableSet.<Predicate>builder()
            .add(ContainsKey, ContainsValue, EntryEq)
            .build();

    private static final Set<Predicate> CQL_COLLECTION_PREDICATES = ImmutableSet.<Predicate>builder()
            .addAll(CQL_MAP_PREDICATES).add(Contains).build();

    public static Class<WhereCondition<?>> type()
    {
        return (Class) WhereCondition.class;
    }

    public enum Predicate
    {
        Eq("="),
        Lt("<"),
        Gt(">"),
        Lte("<="),
        Gte(">="),
        In("IN"),
        Contains("CONTAINS"),
        ContainsKey("CONTAINS KEY"),
        ContainsValue("CONTAINS"),
        EntryEq("="),
        Neq("<>"),
        Without("without");

        private final String cql;

        Predicate(String cql)
        {
            this.cql = cql;
        }

        @Override
        public String toString()
        {
            return cql;
        }

        public boolean isCqlCollectionPredicate()
        {
            return CQL_COLLECTION_PREDICATES.contains(this);
        }

        public boolean isCqlMapPredicate()
        {
            return CQL_MAP_PREDICATES.contains(this);
        }

        public boolean isCQLPredicate()
        {
            return this == Eq || this == Lt || this == Gt || this == Lte || this == Gte || this == Contains
                    || this == ContainsKey || this == In || this == ContainsValue || this == EntryEq;
        }

        public boolean isClusteringPredicate()
        {
            // https://docs.datastax.com/en/dse/6.8/cql/cql/cql_using/useQueryIN.html
            // https://docs.datastax.com/en/dse/6.8/cql/cql/cql_reference/cql_commands/cqlSelect.html
            // "IN: Restricted to the last column of the partition key to search multiple partitions." (under the
            // section partition_conditions)
            // https://docs.datastax.com/en/dse/6.8/cql/cql/cql_using/wherePK.html
            // "Use the following operators for partition key logical statements:
            // * Equals (=)
            // * IN
            // [snip]"
            return this == Eq || this == Lt || this == Gt || this == Lte || this == Gte || this == In;
        }

        public boolean isCompare()
        {
            return this == Eq || this == Neq || this == Lt || this == Gt || this == Lte || this == Gte;
        }

        public boolean isContains()
        {
            return this == In || this == Without;
        }
    }

    public abstract Column column();

    public abstract Optional<String[]> path();

    @Value.Lazy
    public Optional<String> fqPath()
    {
        if (path().isPresent())
        {
            return Optional.of(column().name() + WhereCondition.PATH_DELIMITER
                    + Arrays.asList(path().get()).stream().collect(Collectors.joining(WhereCondition.PATH_DELIMITER)));
        }
        return Optional.empty();
    }

    public abstract Predicate predicate();

    public interface Builder<T>
    {
        default ImmutableWhereCondition.Builder<T> column(String column)
        {
            return column(Column.reference(column));
        }

        ImmutableWhereCondition.Builder<T> column(Column column);

    }

    @Override
    public String toString()
    {
        String value = value().isPresent() ? value().get().toString() : "?";
        String key = column().name();
        if (path().isPresent())
        {
            key = fqPath().get();
        }
        return String.format("%s %s %s", key, predicate().toString(), value);
    }

    @Override
    @Value.Default
    public boolean ignored()
    {
        return false;
    }

    @Override
    public Parameter<T> ignore()
    {
        return ImmutableWhereCondition.<T>builder().from(this).ignored(true).build();
    }
}
