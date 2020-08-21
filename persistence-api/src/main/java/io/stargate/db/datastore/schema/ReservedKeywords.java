package io.stargate.db.datastore.schema;

import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;

public final class ReservedKeywords
{
    @VisibleForTesting
    static final String[] reservedKeywords = new String[]
            {
                    "SELECT",
                    "FROM",
                    "WHERE",
                    "AND",
                    "ENTRIES",
                    "FULL",
                    "INSERT",
                    "UPDATE",
                    "WITH",
                    "LIMIT",
                    "USING",
                    "USE",
                    "SET",
                    "BEGIN",
                    "UNLOGGED",
                    "BATCH",
                    "APPLY",
                    "TRUNCATE",
                    "DELETE",
                    "IN",
                    "CREATE",
                    "KEYSPACE",
                    "SCHEMA",
                    "COLUMNFAMILY",
                    "TABLE",
                    "MATERIALIZED",
                    "VIEW",
                    "INDEX",
                    "ON",
                    "TO",
                    "DROP",
                    "PRIMARY",
                    "INTO",
                    "ALTER",
                    "RENAME",
                    "ADD",
                    "ORDER",
                    "BY",
                    "ASC",
                    "DESC",
                    "ALLOW",
                    "IF",
                    "IS",
                    "GRANT",
                    "OF",
                    "REVOKE",
                    "MODIFY",
                    "AUTHORIZE",
                    "DESCRIBE",
                    "EXECUTE",
                    "NORECURSIVE",
                    "TOKEN",
                    "NULL",
                    "NOT",
                    "NAN",
                    "INFINITY",
                    "OR",
                    "REPLACE",
                    "DEFAULT",
                    "UNSET",
                    "MBEAN",
                    "MBEANS",
                    "FOR",
                    "RESTRICT",
                    "UNRESTRICT"};

    private static final Set<String> reservedSet = new CopyOnWriteArraySet<>(Arrays.asList(reservedKeywords));

    /**
     * This class must not be instantiated as it only contains static methods.
     */
    private ReservedKeywords()
    {
    }

    public static boolean isReserved(String text)
    {
        return reservedSet.contains(text.toUpperCase());
    }

    /**
     * Adds the specified reserved keyword.
     * <p>
     * Used by DSE, see DB-1637.
     */
    public static boolean addReserved(String text)
    {
        return reservedSet.add(text.toUpperCase());
    }
}
