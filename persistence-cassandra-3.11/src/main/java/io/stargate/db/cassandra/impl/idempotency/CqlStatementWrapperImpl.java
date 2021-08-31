package io.stargate.db.cassandra.impl.idempotency;

import io.stargate.db.idempotency.CqlStatementWrapper;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.SchemaAlteringStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.cql3.statements.TruncateStatement;
import org.apache.cassandra.cql3.statements.UseStatement;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class CqlStatementWrapperImpl implements CqlStatementWrapper {
    private final CQLStatement cqlStatement;

    public CqlStatementWrapperImpl(CQLStatement cqlStatement) {
        this.cqlStatement = cqlStatement;
    }

    @Override
    public boolean isBatchStatement() {
        return cqlStatement instanceof BatchStatement;
    }

    @Override
    public List<CqlStatementWrapper> getStatements() {
        if(isBatchStatement()){
            return ((BatchStatement)cqlStatement).getStatements().stream()
                    .map(CqlStatementWrapperImpl::new).collect(Collectors.toList());
        }else {
            return Collections.emptyList();
        }
    }

    @Override
    public boolean isSelectStatement() {
        return cqlStatement instanceof SelectStatement;
    }

    @Override
    public boolean isTruncate() {
        return cqlStatement instanceof TruncateStatement;
    }

    @Override
    public boolean isUse() {
        return cqlStatement instanceof UseStatement;
    }

    @Override
    public boolean isSchemaChange() {
        return cqlStatement instanceof SchemaAlteringStatement;
    }

    @Override
    public boolean isModification() {
        return cqlStatement instanceof ModificationStatement;
    }

    @Override
    public boolean isLwt() {
         if(isModification()) {
             ModificationStatement modification = (ModificationStatement) this.cqlStatement;
             return modification.hasIfExistCondition()
                     || modification.hasIfNotExistCondition()
                     || modification.hasConditions();
         }else{
             return false;
         }
    }

    @Override
    public boolean isCounter() {
        if(isModification()) {
            ModificationStatement modification = (ModificationStatement) this.cqlStatement;
            return modification.isCounter();
        }else{
            return false;
        }
    }
}
