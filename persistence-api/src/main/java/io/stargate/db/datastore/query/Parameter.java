/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package io.stargate.db.datastore.query;

import java.util.Optional;
import java.util.function.Function;

import io.stargate.db.datastore.schema.Column;

public interface Parameter<T> extends Comparable<Parameter>
{
    Column column();

    Optional<Object> value();

    Object UNSET = new SpecialTermMarker()
    {
        final static String term = "<unset>";
        @Override
        public String toString()
        {
            return term;
        }

        @Override
        public int hashCode()
        {
            return term.hashCode();
        }

        @Override
        public boolean equals(Object obj)
        {
            return obj != null && this.toString().equals(obj.toString());
        }
    };

    Object NULL = new SpecialTermMarker()
    {
        static final String term = "<null>";

        @Override
        public String toString()
        {
            return term;
        }

        @Override
        public int hashCode()
        {
            return term.hashCode();
        }

        @Override
        public boolean equals(Object obj)
        {
            return obj != null && this.toString().equals(obj.toString());
        }
    };

    Optional<Function<T, Object>> bindingFunction();

    default int compareTo(Parameter other)
    {
        return column().name().compareTo(other.column().name());
    }

    default boolean ignored()
    {
        return false;
    }

    default Parameter<T> ignore()
    {
        return this;
    }

    interface SpecialTermMarker{}
}
