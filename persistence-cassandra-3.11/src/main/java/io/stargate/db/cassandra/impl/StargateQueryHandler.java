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
import io.stargate.auth.SourceAPI;
import io.stargate.db.cassandra.impl.interceptors.QueryInterceptor;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.cassandra.auth.IResource;
import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.cql3.BatchQueryOptions;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.AlterKeyspaceStatement;
import org.apache.cassandra.cql3.statements.AlterRoleStatement;
import org.apache.cassandra.cql3.statements.AlterTableStatement;
import org.apache.cassandra.cql3.statements.AlterTypeStatement;
import org.apache.cassandra.cql3.statements.AlterViewStatement;
import org.apache.cassandra.cql3.statements.AuthenticationStatement;
import org.apache.cassandra.cql3.statements.AuthorizationStatement;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.CreateAggregateStatement;
import org.apache.cassandra.cql3.statements.CreateFunctionStatement;
import org.apache.cassandra.cql3.statements.CreateIndexStatement;
import org.apache.cassandra.cql3.statements.CreateKeyspaceStatement;
import org.apache.cassandra.cql3.statements.CreateRoleStatement;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.cql3.statements.CreateTriggerStatement;
import org.apache.cassandra.cql3.statements.CreateTypeStatement;
import org.apache.cassandra.cql3.statements.CreateViewStatement;
import org.apache.cassandra.cql3.statements.DeleteStatement;
import org.apache.cassandra.cql3.statements.DropAggregateStatement;
import org.apache.cassandra.cql3.statements.DropFunctionStatement;
import org.apache.cassandra.cql3.statements.DropIndexStatement;
import org.apache.cassandra.cql3.statements.DropKeyspaceStatement;
import org.apache.cassandra.cql3.statements.DropRoleStatement;
import org.apache.cassandra.cql3.statements.DropTableStatement;
import org.apache.cassandra.cql3.statements.DropTriggerStatement;
import org.apache.cassandra.cql3.statements.DropTypeStatement;
import org.apache.cassandra.cql3.statements.DropViewStatement;
import org.apache.cassandra.cql3.statements.GrantPermissionsStatement;
import org.apache.cassandra.cql3.statements.ListPermissionsStatement;
import org.apache.cassandra.cql3.statements.ListRolesStatement;
import org.apache.cassandra.cql3.statements.ListUsersStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.cql3.statements.PermissionsManagementStatement;
import org.apache.cassandra.cql3.statements.RevokePermissionsStatement;
import org.apache.cassandra.cql3.statements.RoleManagementStatement;
import org.apache.cassandra.cql3.statements.SchemaAlteringStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.cql3.statements.TruncateStatement;
import org.apache.cassandra.cql3.statements.UseStatement;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.transport.messages.ResultMessage.Prepared;
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
  public ResultMessage process(
      String queryString,
      QueryState queryState,
      QueryOptions options,
      Map<String, ByteBuffer> customPayload,
      long queryStartNanoTime)
      throws RequestExecutionException, RequestValidationException {

    ParsedStatement.Prepared p =
        QueryProcessor.getStatement(queryString, queryState.getClientState());
    options.prepare(p.boundNames);
    CQLStatement statement = p.statement;
    if (statement.getBoundTerms() != options.getValues().size()) {
      throw new InvalidRequestException(
          String.format(
              "there were %d markers(?) in CQL but %d bound variables",
              statement.getBoundTerms(), options.getValues().size()));
    }

    if (!queryState.getClientState().isInternal) {
      QueryProcessor.metrics.regularStatementsExecuted.inc();
    }

    ResultMessage result =
        maybeIntercept(statement, queryState, options, customPayload, queryStartNanoTime);

    if (result != null) {
      return result;
    }

    if (customPayload != null && customPayload.containsKey("token")) {
      authorizeByToken(customPayload.get("token"), statement);
    }

    return QueryProcessor.instance.processStatement(
        statement, queryState, options, queryStartNanoTime);
  }

  @Override
  public Prepared prepare(String s, QueryState queryState, Map<String, ByteBuffer> customPayload)
      throws RequestValidationException {
    return QueryProcessor.instance.prepare(s, queryState, customPayload);
  }

  @Override
  public ParsedStatement.Prepared getPrepared(MD5Digest id) {
    return QueryProcessor.instance.getPrepared(id);
  }

  @Override
  public ParsedStatement.Prepared getPreparedForThrift(Integer id) {
    return QueryProcessor.instance.getPreparedForThrift(id);
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
    if (customPayload != null && customPayload.containsKey("token")) {
      authorizeByToken(customPayload.get("token"), batchStatement);
    }

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
            authToken, castStatement.keyspace(), castStatement.columnFamily(), SourceAPI.CQL);
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
      authorizeModificationStatement(statement, authToken, authorization);
    } else if (statement instanceof TruncateStatement) {
      TruncateStatement castStatement = (TruncateStatement) statement;
      logger.debug(
          "preparing to authorize statement of type {} on {}.{}",
          castStatement.getClass().toString(),
          castStatement.keyspace(),
          castStatement.columnFamily());

      try {
        authorization.authorizeDataWrite(
            authToken,
            castStatement.keyspace(),
            castStatement.columnFamily(),
            Scope.TRUNCATE,
            SourceAPI.CQL);
      } catch (io.stargate.auth.UnauthorizedException e) {
        throw new UnauthorizedException(
            String.format(
                "No TRUNCATE permission on <table %s.%s>",
                castStatement.keyspace(), castStatement.columnFamily()));
      }

      logger.debug(
          "authorized statement of type {} on {}.{}",
          castStatement.getClass().toString(),
          castStatement.keyspace(),
          castStatement.columnFamily());
    } else if (statement instanceof SchemaAlteringStatement) {
      authorizeSchemaAlteringStatement(statement, authToken, authorization);
    } else if (statement instanceof AuthorizationStatement) {
      authorizeAuthorizationStatement(statement, authToken, authorization);
    } else if (statement instanceof AuthenticationStatement) {
      authorizeAuthenticationStatement(statement, authToken, authorization);
    } else if (statement instanceof UseStatement) {
      // NOOP on UseStatement since it doesn't require authorization
      logger.debug("Skipping auth on UseStatement since it's not required");
    } else if (statement instanceof BatchStatement) {
      BatchStatement castStatement = (BatchStatement) statement;
      List<ModificationStatement> statements = castStatement.getStatements();
      for (ModificationStatement stmt : statements) {
        authorizeModificationStatement(stmt, authToken, authorization);
      }
    } else {
      logger.warn("Tried to authorize unsupported statement");
      throw new UnsupportedOperationException(
          "Unable to authorize statement "
              + (statement != null ? statement.getClass().getName() : "null"));
    }
  }

  private void authorizeModificationStatement(
      CQLStatement statement, String authToken, AuthorizationService authorization) {
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
          authToken, castStatement.keyspace(), castStatement.columnFamily(), scope, SourceAPI.CQL);
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
  }

  private void authorizeSchemaAlteringStatement(
      CQLStatement statement, String authToken, AuthorizationService authorization) {
    SchemaAlteringStatement castStatement = (SchemaAlteringStatement) statement;
    Scope scope = null;
    String keyspaceName = null;
    String tableName = null;

    if (statement instanceof CreateTableStatement) {
      scope = Scope.CREATE;
      keyspaceName = castStatement.keyspace();
      tableName = castStatement.columnFamily();
    } else if (statement instanceof DropTableStatement) {
      scope = Scope.DROP;
      keyspaceName = castStatement.keyspace();
      tableName = castStatement.columnFamily();
    } else if (statement instanceof AlterTableStatement) {
      scope = Scope.ALTER;
      keyspaceName = castStatement.keyspace();
      tableName = castStatement.columnFamily();
    } else if (statement instanceof CreateKeyspaceStatement) {
      scope = Scope.CREATE;
      keyspaceName = castStatement.keyspace();
      tableName = null;
    } else if (statement instanceof DropKeyspaceStatement) {
      scope = Scope.DROP;
      keyspaceName = castStatement.keyspace();
      tableName = null;
    } else if (statement instanceof AlterKeyspaceStatement) {
      scope = Scope.ALTER;
      keyspaceName = castStatement.keyspace();
      tableName = null;
    } else if (statement instanceof AlterTypeStatement) {
      scope = Scope.ALTER;
      keyspaceName = castStatement.keyspace();
    } else if (statement instanceof AlterViewStatement) {
      scope = Scope.ALTER;
      keyspaceName = castStatement.keyspace();
      tableName = castStatement.columnFamily();
    } else if (statement instanceof CreateAggregateStatement) {
      scope = Scope.CREATE;
      keyspaceName = castStatement.keyspace();
    } else if (statement instanceof CreateFunctionStatement) {
      scope = Scope.CREATE;
      keyspaceName = castStatement.keyspace();
    } else if (statement instanceof CreateIndexStatement) {
      scope = Scope.CREATE;
      keyspaceName = castStatement.keyspace();
      tableName = castStatement.columnFamily();
    } else if (statement instanceof CreateTriggerStatement) {
      scope = Scope.CREATE;
      keyspaceName = castStatement.keyspace();
      tableName = castStatement.columnFamily();
    } else if (statement instanceof CreateTypeStatement) {
      scope = Scope.CREATE;
      keyspaceName = castStatement.keyspace();
    } else if (statement instanceof CreateViewStatement) {
      scope = Scope.CREATE;
      keyspaceName = castStatement.keyspace();
      tableName = castStatement.columnFamily();
    } else if (statement instanceof DropAggregateStatement) {
      scope = Scope.DELETE;
      keyspaceName = castStatement.keyspace();
    } else if (statement instanceof DropFunctionStatement) {
      scope = Scope.DELETE;
      keyspaceName = castStatement.keyspace();
    } else if (statement instanceof DropIndexStatement) {
      scope = Scope.DELETE;
      keyspaceName = castStatement.keyspace();
      tableName = castStatement.columnFamily();
    } else if (statement instanceof DropTriggerStatement) {
      scope = Scope.DELETE;
      keyspaceName = castStatement.keyspace();
      tableName = castStatement.columnFamily();
    } else if (statement instanceof DropTypeStatement) {
      scope = Scope.DELETE;
      keyspaceName = castStatement.keyspace();
    } else if (statement instanceof DropViewStatement) {
      scope = Scope.DELETE;
      keyspaceName = castStatement.keyspace();
      tableName = castStatement.columnFamily();
    }

    logger.debug(
        "preparing to authorize statement of type {} on {}.{}",
        castStatement.getClass().toString(),
        keyspaceName,
        tableName);

    try {
      authorization.authorizeSchemaWrite(authToken, keyspaceName, tableName, scope, SourceAPI.CQL);
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

  private void authorizeAuthenticationStatement(
      CQLStatement statement, String authToken, AuthorizationService authorization) {
    AuthenticationStatement castStatement = (AuthenticationStatement) statement;
    Scope scope = null;
    String role = null;

    if (statement instanceof RoleManagementStatement) {
      RoleManagementStatement stmt = (RoleManagementStatement) castStatement;
      scope = Scope.AUTHORIZE;
      role = getRoleResourceFromStatement(stmt, "role");
      String grantee = getRoleResourceFromStatement(stmt, "grantee");
      logger.debug(
          "preparing to authorize statement of type {} on {}",
          castStatement.getClass().toString(),
          role);

      try {
        authorization.authorizeRoleManagement(authToken, role, grantee, scope, SourceAPI.CQL);
      } catch (io.stargate.auth.UnauthorizedException e) {
        throw new UnauthorizedException(
            String.format("Missing correct permission on role %s", role), e);
      }

      logger.debug(
          "authorized statement of type {} on {}", castStatement.getClass().toString(), role);
      return;
    } else if (statement instanceof DropRoleStatement) {
      DropRoleStatement stmt = (DropRoleStatement) castStatement;
      scope = Scope.DROP;
      role = getRoleResourceFromStatement(stmt, "role");
    } else if (statement instanceof CreateRoleStatement) {
      CreateRoleStatement stmt = (CreateRoleStatement) castStatement;
      scope = Scope.CREATE;
      role = getRoleResourceFromStatement(stmt, "role");
    } else if (statement instanceof AlterRoleStatement) {
      AlterRoleStatement stmt = (AlterRoleStatement) castStatement;
      scope = Scope.ALTER;
      role = getRoleResourceFromStatement(stmt, "role");
    }

    logger.debug(
        "preparing to authorize statement of type {} on {}",
        castStatement.getClass().toString(),
        role);

    try {
      authorization.authorizeRoleManagement(authToken, role, scope, SourceAPI.CQL);
    } catch (io.stargate.auth.UnauthorizedException e) {
      throw new UnauthorizedException(
          String.format("Missing correct permission on role %s", role), e);
    }

    logger.debug(
        "authorized statement of type {} on {}", castStatement.getClass().toString(), role);
  }

  private void authorizeAuthorizationStatement(
      CQLStatement statement, String authToken, AuthorizationService authorization) {
    AuthorizationStatement castStatement = (AuthorizationStatement) statement;

    if (statement instanceof PermissionsManagementStatement) {
      PermissionsManagementStatement stmt = (PermissionsManagementStatement) castStatement;
      Scope scope = Scope.AUTHORIZE;
      String resource = getResourceFromStatement(stmt);
      String grantee = getRoleResourceFromStatement(stmt, "grantee");

      logger.debug(
          "preparing to authorize statement of type {} on {}",
          castStatement.getClass().toString(),
          resource);

      try {
        authorization.authorizePermissionManagement(
            authToken, resource, grantee, scope, SourceAPI.CQL);
      } catch (io.stargate.auth.UnauthorizedException e) {
        throw new UnauthorizedException(
            String.format("Missing correct permission on role %s", resource), e);
      }

      logger.debug(
          "authorized statement of type {} on {}", castStatement.getClass().toString(), resource);
    } else if (statement instanceof ListRolesStatement) {
      ListRolesStatement stmt = (ListRolesStatement) castStatement;
      String role = getRoleResourceFromStatement(stmt, "grantee");
      logger.debug(
          "preparing to authorize statement of type {} on {}",
          castStatement.getClass().toString(),
          role);

      try {
        authorization.authorizeRoleRead(authToken, role, SourceAPI.CQL);
      } catch (io.stargate.auth.UnauthorizedException e) {
        throw new UnauthorizedException(
            String.format("Missing correct permission on role %s", role), e);
      }

      logger.debug(
          "authorized statement of type {} on {}", castStatement.getClass().toString(), role);
    } else if (statement instanceof ListPermissionsStatement) {
      ListPermissionsStatement stmt = (ListPermissionsStatement) castStatement;
      String role = getRoleResourceFromStatement(stmt, "grantee");
      logger.debug(
          "preparing to authorize statement of type {} on {}",
          castStatement.getClass().toString(),
          role);

      try {
        authorization.authorizePermissionRead(authToken, role, SourceAPI.CQL);
      } catch (io.stargate.auth.UnauthorizedException e) {
        throw new UnauthorizedException(
            String.format("Missing correct permission on role %s", role), e);
      }

      logger.debug(
          "authorized statement of type {} on {}", castStatement.getClass().toString(), role);
    }
  }

  private String getRoleResourceFromStatement(Object stmt, String fieldName) {
    try {
      Class<?> aClass = stmt.getClass();
      if (stmt instanceof ListUsersStatement
          || stmt instanceof GrantPermissionsStatement
          || stmt instanceof RevokePermissionsStatement) {
        aClass = aClass.getSuperclass();
      }

      Field f = aClass.getDeclaredField(fieldName);
      f.setAccessible(true);
      RoleResource roleResource = (RoleResource) f.get(stmt);

      return roleResource != null ? roleResource.getName() : null;
    } catch (Exception e) {
      logger.error("Unable to get " + fieldName, e);
      throw new RuntimeException("Unable to get private field", e);
    }
  }

  private String getResourceFromStatement(PermissionsManagementStatement stmt) {
    try {
      Field f = stmt.getClass().getSuperclass().getDeclaredField("resource");
      f.setAccessible(true);
      IResource resource = (IResource) f.get(stmt);

      return resource != null ? resource.getName() : null;
    } catch (Exception e) {
      logger.error("Unable to get role", e);
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
