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
package io.stargate.db.cassandra.impl;

import io.stargate.auth.AuthorizationService;
import io.stargate.auth.Scope;
import io.stargate.db.cassandra.impl.interceptors.QueryInterceptor;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
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
import org.apache.cassandra.cql3.statements.schema.AlterTableStatement;
import org.apache.cassandra.cql3.statements.schema.CreateKeyspaceStatement;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.cql3.statements.schema.DropKeyspaceStatement;
import org.apache.cassandra.cql3.statements.schema.DropTableStatement;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.schema.SchemaTransformation;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.MD5Digest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StargateQueryHandler implements QueryHandler {

  private static final Logger logger = LoggerFactory.getLogger(StargateQueryHandler.class);
  private final List<QueryInterceptor> interceptors = new CopyOnWriteArrayList<>();
  private AtomicReference<AuthorizationService> authorizationService;

  void register(QueryInterceptor interceptor) {
    this.interceptors.add(interceptor);
  }

  private ResultMessage maybeIntercept(
      CQLStatement statement,
      QueryState queryState,
      QueryOptions options,
      Map<String, ByteBuffer> customPayload,
      long queryStartNanoTime) {
    for (QueryInterceptor interceptor : interceptors) {
      ResultMessage result =
          interceptor.interceptQuery(
              statement, queryState, options, customPayload, queryStartNanoTime);
      if (result != null) {
        return result;
      }
    }
    return null;
  }

  @Override
  public CQLStatement parse(String s, QueryState queryState, QueryOptions queryOptions) {
    return QueryProcessor.instance.parse(s, queryState, queryOptions);
  }

  @Override
  public ResultMessage process(
      CQLStatement statement,
      QueryState queryState,
      QueryOptions options,
      Map<String, ByteBuffer> customPayload,
      long queryStartNanoTime)
      throws RequestExecutionException, RequestValidationException {
    ResultMessage result =
        maybeIntercept(statement, queryState, options, customPayload, queryStartNanoTime);

    if (result != null) {
      return result;
    }

    if (customPayload != null && customPayload.containsKey("token")) {
      authorizeByToken(customPayload.get("token"), statement);
    }

    return QueryProcessor.instance.process(statement, queryState, options, queryStartNanoTime);
  }

  @Override
  public ResultMessage.Prepared prepare(
      String s, ClientState clientState, Map<String, ByteBuffer> map)
      throws RequestValidationException {
    return QueryProcessor.instance.prepare(s, clientState, map);
  }

  @Override
  public Prepared getPrepared(MD5Digest md5Digest) {
    return QueryProcessor.instance.getPrepared(md5Digest);
  }

  @Override
  public ResultMessage processPrepared(
      CQLStatement statement,
      QueryState queryState,
      QueryOptions options,
      Map<String, ByteBuffer> customPayload,
      long queryStartNanoTime)
      throws RequestExecutionException, RequestValidationException {

    ResultMessage result =
        maybeIntercept(statement, queryState, options, customPayload, queryStartNanoTime);

    if (result != null) {
      return result;
    }

    if (customPayload != null && customPayload.containsKey("token")) {
      authorizeByToken(customPayload.get("token"), statement);
    }

    return QueryProcessor.instance.processPrepared(
        statement, queryState, options, customPayload, queryStartNanoTime);
  }

  @Override
  public ResultMessage processBatch(
      BatchStatement batchStatement,
      QueryState queryState,
      BatchQueryOptions options,
      Map<String, ByteBuffer> customPayload,
      long queryStartNanoTime)
      throws RequestExecutionException, RequestValidationException {
    return QueryProcessor.instance.processBatch(
        batchStatement, queryState, options, customPayload, queryStartNanoTime);
  }

  private void authorizeByToken(ByteBuffer token, CQLStatement statement) {
    String authToken = StandardCharsets.UTF_8.decode(token).toString();

    if (!getAuthorizationService().isPresent()) {
      throw new RuntimeException(
          "Failed to find an io.stargate.auth.AuthorizationService to authorize request");
    }

    AuthorizationService authorization = getAuthorizationService().get();
    if (statement instanceof SelectStatement) {
      SelectStatement castStatement = (SelectStatement) statement;
      logger.debug(
          "preparing to authorize statement of type {} on {}.{}",
          castStatement.getClass().toString(),
          castStatement.keyspace(),
          castStatement.columnFamily());

      try {
        authorization.authorizeDataRead(
            authToken, castStatement.keyspace(), castStatement.columnFamily());
      } catch (io.stargate.auth.UnauthorizedException e) {
        throw new UnauthorizedException(
            String.format(
                "No SELECT permission on <table %s.%s>",
                castStatement.keyspace(), castStatement.columnFamily()));
      }

      logger.debug(
          "authorized statement of type {} on {}.{}",
          castStatement.getClass().toString(),
          castStatement.keyspace(),
          castStatement.columnFamily());
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
          castStatement.columnFamily());

      try {
        authorization.authorizeDataWrite(
            authToken, castStatement.keyspace(), castStatement.columnFamily(), scope);
      } catch (io.stargate.auth.UnauthorizedException e) {
        throw new UnauthorizedException(
            String.format(
                "Missing correct permission on <table %s.%s>",
                castStatement.keyspace(), castStatement.columnFamily()));
      }

      logger.debug(
          "authorized statement of type {} on {}.{}",
          castStatement.getClass().toString(),
          castStatement.keyspace(),
          castStatement.columnFamily());
    } else if (statement instanceof TruncateStatement) {
      TruncateStatement castStatement = (TruncateStatement) statement;

      logger.debug(
          "preparing to authorize statement of type {} on {}.{}",
          castStatement.getClass().toString(),
          castStatement.keyspace(),
          castStatement.name());

      try {
        authorization.authorizeSchemaWrite(
            authToken, castStatement.keyspace(), castStatement.name(), Scope.TRUNCATE);
      } catch (io.stargate.auth.UnauthorizedException e) {
        throw new UnauthorizedException(
            String.format(
                "No TRUNCATE permission on <table %s.%s>",
                castStatement.keyspace(), castStatement.name()));
      }

      logger.debug(
          "authorized statement of type {} on {}.{}",
          castStatement.getClass().toString(),
          castStatement.keyspace(),
          castStatement.name());
    } else if (statement instanceof SchemaTransformation) {
      SchemaTransformation castStatement = (SchemaTransformation) statement;
      Scope scope = null;
      String keyspaceName = null;
      String tableName = null;

      if (statement instanceof CreateTableStatement) {
        scope = Scope.CREATE;

        CreateTableStatement stmt = (CreateTableStatement) statement;
        keyspaceName = getKeyspaceName(stmt);
        tableName = getTableName(stmt);
      } else if (statement instanceof DropTableStatement) {
        scope = Scope.DELETE;

        DropTableStatement stmt = (DropTableStatement) statement;
        keyspaceName = getKeyspaceName(stmt);
        tableName = getTableName(stmt);
      } else if (statement instanceof AlterTableStatement) {
        scope = Scope.ALTER;

        AlterTableStatement stmt = (AlterTableStatement) statement;
        keyspaceName = getKeyspaceName(stmt);
        tableName = getTableName(stmt);
      } else if (statement instanceof CreateKeyspaceStatement) {
        scope = Scope.CREATE;

        CreateKeyspaceStatement stmt = (CreateKeyspaceStatement) statement;
        keyspaceName = getKeyspaceName(stmt);
      } else if (statement instanceof DropKeyspaceStatement) {
        scope = Scope.DELETE;

        DropKeyspaceStatement stmt = (DropKeyspaceStatement) statement;
        keyspaceName = getKeyspaceName(stmt);
      } else if (statement instanceof AlterKeyspaceStatement) {
        scope = Scope.ALTER;

        AlterKeyspaceStatement stmt = (AlterKeyspaceStatement) statement;
        keyspaceName = getKeyspaceName(stmt);
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
  }

  private String getTableName(Object stmt) {
    try {
      Field f = stmt.getClass().getDeclaredField("tableName");
      f.setAccessible(true);
      return (String) f.get(stmt);
    } catch (Exception e) {
      logger.error("Unable to get private field", e);
      throw new RuntimeException("Unable to get private field", e);
    }
  }

  private String getKeyspaceName(Object stmt) {
    try {
      Field f = stmt.getClass().getSuperclass().getDeclaredField("keyspaceName");
      f.setAccessible(true);
      return (String) f.get(stmt);
    } catch (Exception e) {
      logger.error("Unable to get private field", e);
      throw new RuntimeException("Unable to get private field", e);
    }
  }

  public void setAuthorizationService(AtomicReference<AuthorizationService> authorizationService) {
    this.authorizationService = authorizationService;
  }

  public Optional<AuthorizationService> getAuthorizationService() {
    return Optional.ofNullable(authorizationService.get());
  }
}
