/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.db.dse.impl;

import io.reactivex.Single;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.Scope;
import io.stargate.db.dse.impl.interceptors.QueryInterceptor;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.cassandra.cql3.BatchQueryOptions;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.DeleteStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.cql3.statements.TruncateStatement;
import org.apache.cassandra.cql3.statements.schema.AlterKeyspaceStatement;
import org.apache.cassandra.cql3.statements.schema.AlterSchemaStatement;
import org.apache.cassandra.cql3.statements.schema.AlterTableStatement;
import org.apache.cassandra.cql3.statements.schema.CreateKeyspaceStatement;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.cql3.statements.schema.DropKeyspaceStatement;
import org.apache.cassandra.cql3.statements.schema.DropTableStatement;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.MD5Digest;
import org.osgi.framework.BundleContext;
import org.osgi.framework.FrameworkUtil;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StargateQueryHandler implements QueryHandler {

  private static final Logger logger = LoggerFactory.getLogger(StargateQueryHandler.class);
  private final List<QueryInterceptor> interceptors = new CopyOnWriteArrayList<>();
  private ServiceReference<?> authorizationReference;

  public void register(QueryInterceptor interceptor) {
    this.interceptors.add(interceptor);
  }

  @Override
  public Single<ResultMessage> process(
      String query,
      QueryState queryState,
      QueryOptions options,
      Map<String, ByteBuffer> customPayload,
      long queryStartNanoTime) {

    QueryState state = queryState.cloneWithKeyspaceIfSet(options.getKeyspace());
    CQLStatement statement;
    try {
      statement = QueryProcessor.getStatement(query, state);
      options.prepare(statement.getBindVariables());
    } catch (Exception e) {
      return QueryProcessor.auditLogger.logFailedQuery(query, state, e).andThen(Single.error(e));
    }

    if (!queryState.isSystem()) QueryProcessor.metrics.regularStatementsExecuted.inc();

    return processStatement(statement, state, options, customPayload, queryStartNanoTime);
  }

  @Override
  public Single<ResultMessage.Prepared> prepare(
      String query, QueryState queryState, Map<String, ByteBuffer> customPayload) {
    return QueryProcessor.instance.prepare(query, queryState, customPayload);
  }

  @Override
  public Prepared getPrepared(MD5Digest id) {
    return QueryProcessor.instance.getPrepared(id);
  }

  @Override
  public Single<ResultMessage> processStatement(
      CQLStatement statement,
      QueryState queryState,
      QueryOptions options,
      Map<String, ByteBuffer> customPayload,
      long queryStartNanoTime) {

    for (QueryInterceptor interceptor : interceptors) {
      Single<ResultMessage> result =
          interceptor.interceptQuery(
              statement, queryState, options, customPayload, queryStartNanoTime);
      if (result != null) {
        return result;
      }
    }

    if (customPayload != null && customPayload.containsKey("token")) {
      authorizeByToken(customPayload.get("token"), statement);
    }

    return QueryProcessor.instance.processStatement(
        statement, queryState, options, customPayload, queryStartNanoTime);
  }

  @Override
  public Single<ResultMessage> processBatch(
      BatchStatement batchStatement,
      QueryState queryState,
      BatchQueryOptions options,
      Map<String, ByteBuffer> customPayload,
      long queryStartNanoTime) {
    return QueryProcessor.instance.processBatch(
        batchStatement, queryState, options, customPayload, queryStartNanoTime);
  }

  private void authorizeByToken(ByteBuffer token, CQLStatement statement) {
    String authToken = StandardCharsets.UTF_8.decode(token).toString();

    AuthorizationService authorization = null;
    try {
      authorization = getAuthorizationServiceFromContext();
      if (authorization == null) {
        throw new RuntimeException(
            "Failed to find an io.stargate.auth.AuthorizationService to authorize request");
      }

      if (statement instanceof SelectStatement) {
        SelectStatement castStatement = (SelectStatement) statement;
        logger.debug(
            "preparing to authorize statement of type {} on {}.{}",
            castStatement.getClass().toString(),
            castStatement.keyspace(),
            castStatement.table());

        try {
          authorization.authorizeDataRead(
              authToken, castStatement.keyspace(), castStatement.table());
        } catch (io.stargate.auth.UnauthorizedException e) {
          throw new UnauthorizedException(
              String.format(
                  "No SELECT permission on <table %s.%s>",
                  castStatement.keyspace(), castStatement.table()));
        }

        logger.debug(
            "authorized statement of type {} on {}.{}",
            castStatement.getClass().toString(),
            castStatement.keyspace(),
            castStatement.table());
      } else if (statement instanceof ModificationStatement) {
        ModificationStatement castStatement = (ModificationStatement) statement;
        Scope scope;
        if (statement instanceof DeleteStatement) {
          scope = Scope.DELETE;
        } else {
          scope = Scope.MODIFY;
        }

        logger.debug(
            "preparing to authorize statement of type {} on {}.{}",
            castStatement.getClass().toString(),
            castStatement.keyspace(),
            castStatement.table());

        try {
          authorization.authorizeDataWrite(
              authToken, castStatement.keyspace(), castStatement.table(), scope);
        } catch (io.stargate.auth.UnauthorizedException e) {
          throw new UnauthorizedException(
              String.format(
                  "Missing correct permission on <table %s.%s>",
                  castStatement.keyspace(), castStatement.table()));
        }

        logger.debug(
            "authorized statement of type {} on {}.{}",
            castStatement.getClass().toString(),
            castStatement.keyspace(),
            castStatement.table());
      } else if (statement instanceof TruncateStatement) {
        TruncateStatement castStatement = (TruncateStatement) statement;
        logger.debug(
            "preparing to authorize statement of type {} on {}.{}",
            castStatement.getClass().toString(),
            castStatement.keyspace(),
            castStatement.table());

        try {
          authorization.authorizeSchemaWrite(
              authToken, castStatement.keyspace(), castStatement.table(), Scope.TRUNCATE);
        } catch (io.stargate.auth.UnauthorizedException e) {
          throw new UnauthorizedException(
              String.format(
                  "No TRUNCATE permission on <table %s.%s>",
                  castStatement.keyspace(), castStatement.table()));
        }

        logger.debug(
            "authorized statement of type {} on {}.{}",
            castStatement.getClass().toString(),
            castStatement.keyspace(),
            castStatement.table());
      } else if (statement instanceof AlterSchemaStatement) {
        AlterSchemaStatement castStatement = (AlterSchemaStatement) statement;
        Scope scope = null;
        String keyspaceName = null;
        String tableName = null;

        if (statement instanceof CreateTableStatement) {
          scope = Scope.CREATE;
          keyspaceName = castStatement.keyspace();
          tableName = ((CreateTableStatement) castStatement).table();
        } else if (statement instanceof DropTableStatement) {
          scope = Scope.DELETE;
          keyspaceName = castStatement.keyspace();
          tableName = ((DropTableStatement) castStatement).table();
        } else if (statement instanceof AlterTableStatement) {
          scope = Scope.ALTER;
          keyspaceName = castStatement.keyspace();
          tableName = ((AlterTableStatement) castStatement).table();
        } else if (statement instanceof CreateKeyspaceStatement) {
          scope = Scope.CREATE;
          keyspaceName = castStatement.keyspace();
          tableName = null;
        } else if (statement instanceof DropKeyspaceStatement) {
          scope = Scope.DELETE;
          keyspaceName = castStatement.keyspace();
          tableName = null;
        } else if (statement instanceof AlterKeyspaceStatement) {
          scope = Scope.ALTER;
          keyspaceName = castStatement.keyspace();
          tableName = null;
        }

        logger.debug(
            "preparing to authorize statement of type {} on {}.{}",
            castStatement.getClass().toString(),
            keyspaceName,
            tableName);

        try {
          authorization.authorizeSchemaWrite(authToken, keyspaceName, tableName, scope);
        } catch (io.stargate.auth.UnauthorizedException e) {
          throw new UnauthorizedException(
              String.format(
                  "Missing correct permission on %s.%s",
                  keyspaceName, (tableName == null ? "" : tableName)));
        }

        logger.debug(
            "authorized statement of type {} on {}.{}",
            castStatement.getClass().toString(),
            keyspaceName,
            tableName);
      }
    } finally {
      if (authorization != null) {
        ungetAuthorizationService();
      }
    }
  }

  private AuthorizationService getAuthorizationServiceFromContext() {
    BundleContext bundleContext = FrameworkUtil.getBundle(this.getClass()).getBundleContext();

    ServiceReference<?>[] refs;
    try {
      refs = bundleContext.getServiceReferences(AuthorizationService.class.getName(), null);
    } catch (InvalidSyntaxException e) {
      logger.error("Failed to get service reference for AuthorizationService", e);
      return null;
    }

    if (refs != null) {
      for (ServiceReference<?> ref : refs) {
        Object service = bundleContext.getService(ref);
        if (service instanceof AuthorizationService
            && ref.getProperty("AuthIdentifier") != null
            && ref.getProperty("AuthIdentifier").equals(System.getProperty("stargate.auth_id"))) {
          authorizationReference = ref;
          return (AuthorizationService) service;
        } else {
          bundleContext.ungetService(ref);
        }
      }
    }

    return null;
  }

  private void ungetAuthorizationService() {
    BundleContext bundleContext = FrameworkUtil.getBundle(this.getClass()).getBundleContext();
    bundleContext.ungetService(authorizationReference);
  }
}
