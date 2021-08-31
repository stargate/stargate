package io.stargate.db.idempotency;

import java.util.List;

public interface CqlStatementWrapper {
    boolean isBatchStatement();
    List<CqlStatementWrapper> getStatements();
    boolean isSelectStatement();
    boolean isTruncate();
    boolean isUse();
    boolean isSchemaChange();
    boolean isModification();
    boolean isLwt();
    boolean isCounter();
    // many more methods needed...
}
