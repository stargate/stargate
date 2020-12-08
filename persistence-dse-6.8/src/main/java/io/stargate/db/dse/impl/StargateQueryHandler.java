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
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
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
import org.apache.cassandra.cql3.statements.AlterRoleStatement;
import org.apache.cassandra.cql3.statements.AuthenticationStatement;
import org.apache.cassandra.cql3.statements.AuthorizationStatement;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.CreateRoleStatement;
import org.apache.cassandra.cql3.statements.DeleteStatement;
import org.apache.cassandra.cql3.statements.DropRoleStatement;
import org.apache.cassandra.cql3.statements.GrantPermissionsStatement;
import org.apache.cassandra.cql3.statements.GrantRoleStatement;
import org.apache.cassandra.cql3.statements.ListPermissionsStatement;
import org.apache.cassandra.cql3.statements.ListRolesStatement;
import org.apache.cassandra.cql3.statements.ListUsersStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.PermissionsManagementStatement;
import org.apache.cassandra.cql3.statements.PermissionsRelatedStatement;
import org.apache.cassandra.cql3.statements.RevokePermissionsStatement;
import org.apache.cassandra.cql3.statements.RevokeRoleStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.cql3.statements.TruncateStatement;
import org.apache.cassandra.cql3.statements.UseStatement;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StargateQueryHandler implements QueryHandler {

  private static final Logger logger = LoggerFactory.getLogger(StargateQueryHandler.class);
  private final List<QueryInterceptor> interceptors = new CopyOnWriteArrayList<>();
  private AtomicReference<AuthorizationService> authorizationService;

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
          castStatement.table());

      try {
        authorization.authorizeDataRead(authToken, castStatement.keyspace(), castStatement.table());
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
      authorizeModificationStatement(statement, authToken, authorization);
    } else if (statement instanceof TruncateStatement) {
      TruncateStatement castStatement = (TruncateStatement) statement;
      logger.debug(
          "preparing to authorize statement of type {} on {}.{}",
          castStatement.getClass().toString(),
          castStatement.keyspace(),
          castStatement.table());

      try {
        authorization.authorizeDataWrite(
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
      authorizeAlterSchemaStatement(statement, authToken, authorization);
    } else if (statement instanceof AuthorizationStatement) {
      authorizeAuthorizationStatement(statement, authToken, authorization);
    } else if (statement instanceof AuthenticationStatement) {
      authorizeAuthenticationStatement(statement, authToken, authorization);
    } else if (statement instanceof UseStatement) {
      UseStatement castStatement = (UseStatement) statement;
      logger.debug(
          "preparing to authorize statement of type {} on {}",
          castStatement.getClass().toString(),
          castStatement.keyspace());

      try {
        authorization.authorizeSchemaRead(
            authToken, Collections.singletonList(castStatement.keyspace()), null);
      } catch (io.stargate.auth.UnauthorizedException e) {
        throw new UnauthorizedException(
            String.format("No SELECT permission on <keyspace %s>", castStatement.keyspace()));
      }

      logger.debug(
          "authorized statement of type {} on {}",
          castStatement.getClass().toString(),
          castStatement.keyspace());
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
  }

  private void authorizeAuthenticationStatement(
      CQLStatement statement, String authToken, AuthorizationService authorization) {
    AuthenticationStatement castStatement = (AuthenticationStatement) statement;
    Scope scope = null;
    String role = null;

    if (statement instanceof RevokeRoleStatement || statement instanceof GrantRoleStatement) {
      scope = Scope.AUTHORIZE;
      role = getRoleResourceFromStatement(castStatement, "role");
      String grantee = getRoleResourceFromStatement(castStatement, "grantee");
      logger.debug(
          "preparing to authorize statement of type {} on {}",
          castStatement.getClass().toString(),
          role);

      try {
        authorization.authorizeRoleManagement(authToken, role, grantee, scope);
      } catch (io.stargate.auth.UnauthorizedException e) {
        throw new UnauthorizedException(
            String.format("Missing correct permission on role %s", role));
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
      authorization.authorizeRoleManagement(authToken, role, scope);
    } catch (io.stargate.auth.UnauthorizedException e) {
      throw new UnauthorizedException(String.format("Missing correct permission on role %s", role));
    }

    logger.debug(
        "authorized statement of type {} on {}", castStatement.getClass().toString(), role);
  }

  private void authorizeAuthorizationStatement(
      CQLStatement statement, String authToken, AuthorizationService authorization) {
    AuthorizationStatement castStatement = (AuthorizationStatement) statement;

    if (statement instanceof PermissionsRelatedStatement) {
      if (statement instanceof PermissionsManagementStatement) {
        PermissionsRelatedStatement stmt = (PermissionsRelatedStatement) castStatement;
        Scope scope = Scope.AUTHORIZE;
        String resource = getResourceFromStatement(stmt);
        String grantee = getRoleResourceFromStatement(stmt, "grantee");

        logger.debug(
            "preparing to authorize statement of type {} on {}",
            castStatement.getClass().toString(),
            resource);

        try {
          authorization.authorizePermissionManagement(authToken, resource, grantee, scope);
        } catch (io.stargate.auth.UnauthorizedException e) {
          throw new UnauthorizedException(
              String.format("Missing correct permission on role %s", resource));
        }

        logger.debug(
            "authorized statement of type {} on {}", castStatement.getClass().toString(), resource);
      } else if (statement instanceof ListPermissionsStatement) {
        ListPermissionsStatement stmt = (ListPermissionsStatement) castStatement;
        String role = getRoleResourceFromStatement(stmt, "grantee");
        logger.debug(
            "preparing to authorize statement of type {} on {}",
            castStatement.getClass().toString(),
            role);

        try {
          authorization.authorizePermissionRead(authToken, role);
        } catch (io.stargate.auth.UnauthorizedException e) {
          throw new UnauthorizedException(
              String.format("Missing correct permission on role %s", role));
        }

        logger.debug(
            "authorized statement of type {} on {}", castStatement.getClass().toString(), role);
      }
    } else if (statement instanceof ListRolesStatement) {
      ListRolesStatement stmt = (ListRolesStatement) castStatement;
      String role = getRoleResourceFromStatement(stmt, "grantee");
      logger.debug(
          "preparing to authorize statement of type {} on {}",
          castStatement.getClass().toString(),
          role);

      try {
        authorization.authorizeRoleRead(authToken, role);
      } catch (io.stargate.auth.UnauthorizedException e) {
        throw new UnauthorizedException(
            String.format("Missing correct permission on role %s", role));
      }

      logger.debug(
          "authorized statement of type {} on {}", castStatement.getClass().toString(), role);
    }
  }

  private void authorizeAlterSchemaStatement(
      CQLStatement statement, String authToken, AuthorizationService authorization) {
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

  private String getRoleResourceFromStatement(Object stmt, String fieldName) {
    try {
      Class<?> aClass = stmt.getClass();
      if (stmt instanceof ListUsersStatement || stmt instanceof ListPermissionsStatement) {
        aClass = aClass.getSuperclass();
      } else if (stmt instanceof GrantPermissionsStatement
          || stmt instanceof RevokePermissionsStatement) {
        aClass = aClass.getSuperclass().getSuperclass();
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

  private String getResourceFromStatement(PermissionsRelatedStatement stmt) {
    try {
      // Incoming class will be subclass of
      // org.apache.cassandra.cql3.statements.PermissionsManagementStatement but we need the field
      // on it's parent which is org.apache.cassandra.cql3.statements.PermissionsRelatedStatement
      Class<?> superclass = stmt.getClass().getSuperclass().getSuperclass();
      Field f = superclass.getDeclaredField("resource");
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
