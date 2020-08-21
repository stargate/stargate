/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package io.stargate.db.datastore.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.apache.cassandra.stargate.db.ConsistencyLevel;

import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.PreparedStatement;
import io.stargate.db.datastore.ResultSet;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;

/**
 * Mixes in prepared parameters to those supplied at execution.
 */
public class MixinPreparedStatement<T> implements PreparedStatement
{
    private final long unboundParameters;
    private PreparedStatement prepared;
    private List<Parameter<T>> parameters;

    public MixinPreparedStatement(PreparedStatement prepared, List<Parameter<T>> parameters)
    {
        Preconditions.checkNotNull(prepared);
        this.prepared = prepared;
        this.parameters = parameters;
        unboundParameters = parameters.stream().filter(p -> !p.value().isPresent()).count();
        boolean bindingFunctionsFound = parameters.stream().anyMatch(p -> p.bindingFunction().isPresent());
        boolean arrayParametersFound = parameters.stream()
                .anyMatch(p -> !p.value().isPresent() && !p.bindingFunction().isPresent());
        Preconditions.checkArgument(
                bindingFunctionsFound ^ arrayParametersFound || !bindingFunctionsFound && !arrayParametersFound,
                "Mixin prepared statement may not have dynamic bindings and array bindings");
    }

    @Override
    public CompletableFuture<ResultSet> execute(DataStore dataStore, Optional<ConsistencyLevel> consistencyLevel,
                                                Object... parameters)
    {
        Preconditions.checkArgument(parameters.length == unboundParameters,
                "Unexpected number of arguments. Expected %s but got %s. Statement: %s.",
                unboundParameters, parameters.length, prepared);
        List<Object> mergedParameters = new ArrayList<>(this.parameters.size());
        int mergeCount = 0;
        for (Parameter<T> parameter : this.parameters)
        {
            if (parameter.ignored())
            {
                if (!parameter.value().isPresent())
                {
                    mergeCount++;
                }
                continue;
            }
            if (parameter.value().isPresent())
            {
                mergedParameters.add(parameter.value().get());
            }
            else
            {
                mergedParameters.add(parameters[mergeCount++]);
            }
        }
        return prepared.execute(dataStore, consistencyLevel, mergedParameters.toArray());
    }

    public CompletableFuture<ResultSet> execute(DataStore dataStore, T binding)
    {
        return execute(dataStore, Optional.empty(), binding);
    }

    public CompletableFuture<ResultSet> execute(DataStore dataStore, Optional<ConsistencyLevel> consistencyLevel,
                                     T bindingFunctionInput)
    {
        List<Object> mergedParameters = evaluateBindingFunction(bindingFunctionInput);
        return prepared.execute(dataStore, consistencyLevel, mergedParameters.toArray());
    }

    public List<Object> evaluateBindingFunction(T bindingFunctionInput)
    {
        final List<Object> mergedParameters = new ArrayList<>(this.parameters.size());
        for (Parameter<T> parameter : this.parameters)
        {
            if (parameter.ignored())
            {
                continue;
            }
            if (parameter.value().isPresent())
            {
                mergedParameters.add(parameter.value().get());
            }
            else
            {
                /*
                 * Getting an empty Optional throws NoSuchElement exception. The calling code may mistake that
                 * NoSuchElement exception for an exhausted traversal iterator, swallowing the exception. For example,
                 * say we've been called from CoreEngine's vertexRemoved event handler. A NoSuchElementException thrown
                 * here will be silently swallowed, MutationListener.finish will not be called, and the user will see no
                 * exception. Their deletion traversal will appear to have iterated normally despite failing here.
                 *
                 * Throw IllegalStateException instead to make this exception visible to the user/test.
                 *
                 * This could also be an IllegalArgumentException, but this seems likelier to be related to parameter
                 * configuration than to the method args, which makes ISE seem fitting.
                 */
                Preconditions.checkState(parameter.bindingFunction().isPresent(),
                        "No binding function: %s", parameter.column().name());
                mergedParameters.add(parameter.bindingFunction().get().apply(bindingFunctionInput));
            }
        }
        return mergedParameters;
    }

    public PreparedStatement getPrepared()
    {
        return prepared;
    }

    @Override
    public String toString()
    {
        return prepared.toString();
    }
}
