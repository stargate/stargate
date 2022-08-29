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

import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import io.reactivex.Single;
import io.stargate.auth.AuthenticationSubject;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.Scope;
import io.stargate.auth.SourceAPI;
import io.stargate.auth.entity.ResourceKind;
import io.stargate.db.AuthenticatedUser;
import io.stargate.db.AuthenticatedUser.Serializer;
import io.stargate.db.dse.impl.idempotency.IdempotencyAnalyzer;
import io.stargate.db.dse.impl.interceptors.QueryInterceptor;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import javax.validation.constraints.NotNull;
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
import org.apache.cassandra.cql3.statements.schema.AlterTypeStatement;
import org.apache.cassandra.cql3.statements.schema.AlterViewStatement;
import org.apache.cassandra.cql3.statements.schema.CreateAggregateStatement;
import org.apache.cassandra.cql3.statements.schema.CreateFunctionStatement;
import org.apache.cassandra.cql3.statements.schema.CreateIndexStatement;
import org.apache.cassandra.cql3.statements.schema.CreateKeyspaceStatement;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.cql3.statements.schema.CreateTriggerStatement;
import org.apache.cassandra.cql3.statements.schema.CreateTypeStatement;
import org.apache.cassandra.cql3.statements.schema.CreateViewStatement;
import org.apache.cassandra.cql3.statements.schema.DropAggregateStatement;
import org.apache.cassandra.cql3.statements.schema.DropFunctionStatement;
import org.apache.cassandra.cql3.statements.schema.DropIndexStatement;
import org.apache.cassandra.cql3.statements.schema.DropKeyspaceStatement;
import org.apache.cassandra.cql3.statements.schema.DropTableStatement;
import org.apache.cassandra.cql3.statements.schema.DropTriggerStatement;
import org.apache.cassandra.cql3.statements.schema.DropTypeStatement;
import org.apache.cassandra.cql3.statements.schema.DropViewStatement;
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
    return QueryProcessor.instance
        .prepare(query, queryState, customPayload)
        .map(
            p -> {
              Prepared prepared = QueryProcessor.instance.getPrepared(p.statementId);
              CQLStatement statement = prepared.statement;
              boolean idempotent = IdempotencyAnalyzer.isIdempotent(statement);
              boolean useKeyspace = statement instanceof UseStatement;
              return new PreparedWithInfo(p, idempotent, useKeyspace);
            });
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

    if (customPayload != null && customPayload.containsKey("stargate.auth.subject.token")) {
      authorizeByToken(customPayload, statement);
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
    if (customPayload != null && customPayload.containsKey("stargate.auth.subject.token")) {
      authorizeByToken(customPayload, batchStatement);
    }

    return QueryProcessor.instance.processBatch(
        batchStatement, queryState, options, customPayload, queryStartNanoTime);
  }

  @VisibleForTesting
  protected void authorizeByToken(Map<String, ByteBuffer> customPayload, CQLStatement statement) {
    AuthenticationSubject authenticationSubject = loadAuthenticationSubject(customPayload);

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
        authorization.authorizeDataRead(
            authenticationSubject, castStatement.keyspace(), castStatement.table(), SourceAPI.CQL);
      } catch (io.stargate.auth.UnauthorizedException e) {
        throw new UnauthorizedException(
            String.format(
                "No SELECT permission on <table %s.%s>",
                castStatement.keyspace(), castStatement.table()));
      }

      logger.debug(
          "authorized statement of type {} on {}.{}",
          castStatement.getClass(),
          castStatement.keyspace(),
          castStatement.table());
    } else if (statement instanceof ModificationStatement) {
      authorizeModificationStatement(statement, authenticationSubject, authorization);
    } else if (statement instanceof TruncateStatement) {
      TruncateStatement castStatement = (TruncateStatement) statement;
      logger.debug(
          "preparing to authorize statement of type {} on {}.{}",
          castStatement.getClass().toString(),
          castStatement.keyspace(),
          castStatement.table());

      try {
        authorization.authorizeDataWrite(
            authenticationSubject,
            castStatement.keyspace(),
            castStatement.table(),
            Scope.TRUNCATE,
            SourceAPI.CQL);
      } catch (io.stargate.auth.UnauthorizedException e) {
        throw new UnauthorizedException(
            String.format(
                "No TRUNCATE permission on <table %s.%s>",
                castStatement.keyspace(), castStatement.table()));
      }

      logger.debug(
          "authorized statement of type {} on {}.{}",
          castStatement.getClass(),
          castStatement.keyspace(),
          castStatement.table());
    } else if (statement instanceof AlterSchemaStatement) {
      authorizeAlterSchemaStatement(statement, authenticationSubject, authorization);
    } else if (statement instanceof AuthorizationStatement) {
      authorizeAuthorizationStatement(statement, authenticationSubject, authorization);
    } else if (statement instanceof AuthenticationStatement) {
      authorizeAuthenticationStatement(statement, authenticationSubject, authorization);
    } else if (statement instanceof UseStatement) {
      // NOOP on UseStatement since it doesn't require authorization
      logger.debug("Skipping auth on UseStatement since it's not required");
    } else if (statement instanceof BatchStatement) {
      BatchStatement castStatement = (BatchStatement) statement;
      List<ModificationStatement> statements = castStatement.getStatements();
      for (ModificationStatement stmt : statements) {
        authorizeModificationStatement(stmt, authenticationSubject, authorization);
      }
    } else {
      logger.warn("Tried to authorize unsupported statement");
      throw new UnsupportedOperationException(
          "Unable to authorize statement "
              + (statement != null ? statement.getClass().getName() : "null"));
    }
  }

  @NotNull
  private AuthenticationSubject loadAuthenticationSubject(Map<String, ByteBuffer> customPayload) {
    AuthenticatedUser user = Serializer.load(customPayload);
    return AuthenticationSubject.of(user);
  }

  private void authorizeModificationStatement(
      CQLStatement statement,
      AuthenticationSubject authenticationSubject,
      AuthorizationService authorization) {
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
          authenticationSubject,
          castStatement.keyspace(),
          castStatement.table(),
          scope,
          SourceAPI.CQL);
    } catch (io.stargate.auth.UnauthorizedException e) {
      throw new UnauthorizedException(
          String.format(
              "Missing correct permission on <table %s.%s>",
              castStatement.keyspace(), castStatement.table()));
    }

    logger.debug(
        "authorized statement of type {} on {}.{}",
        castStatement.getClass(),
        castStatement.keyspace(),
        castStatement.table());
  }

  private void authorizeAuthenticationStatement(
      CQLStatement statement,
      AuthenticationSubject authenticationSubject,
      AuthorizationService authorization) {
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
        authorization.authorizeRoleManagement(
            authenticationSubject, role, grantee, scope, SourceAPI.CQL);
      } catch (io.stargate.auth.UnauthorizedException e) {
        logger.debug("Unauthorized statement: " + scope, e);
        throw new UnauthorizedException(
            String.format("Missing correct permission on role %s: %s", role, e.getMessage()));
      }

      logger.debug("authorized statement of type {} on {}", castStatement.getClass(), role);
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
      authorization.authorizeRoleManagement(authenticationSubject, role, scope, SourceAPI.CQL);
    } catch (io.stargate.auth.UnauthorizedException e) {
      logger.debug("Unauthorized statement: " + scope, e);
      throw new UnauthorizedException(
          String.format("Missing correct permission on role %s: %s", role, e.getMessage()));
    }

    logger.debug("authorized statement of type {} on {}", castStatement.getClass(), role);
  }

  private void authorizeAuthorizationStatement(
      CQLStatement statement,
      AuthenticationSubject authenticationSubject,
      AuthorizationService authorization) {
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
          authorization.authorizePermissionManagement(
              authenticationSubject, resource, grantee, scope, SourceAPI.CQL);
        } catch (io.stargate.auth.UnauthorizedException e) {
          logger.debug("Unauthorized statement: " + scope, e);
          throw new UnauthorizedException(
              String.format("Missing correct permission on role %s: %s", resource, e.getMessage()));
        }

        logger.debug("authorized statement of type {} on {}", castStatement.getClass(), resource);
      } else if (statement instanceof ListPermissionsStatement) {
        ListPermissionsStatement stmt = (ListPermissionsStatement) castStatement;
        String role = getRoleResourceFromStatement(stmt, "grantee");
        logger.debug(
            "preparing to authorize statement of type {} on {}",
            castStatement.getClass().toString(),
            role);

        try {
          authorization.authorizePermissionRead(authenticationSubject, role, SourceAPI.CQL);
        } catch (io.stargate.auth.UnauthorizedException e) {
          logger.debug("Unauthorized statement", e);
          throw new UnauthorizedException(
              String.format("Missing correct permission on role %s: %s", role, e.getMessage()));
        }

        logger.debug("authorized statement of type {} on {}", castStatement.getClass(), role);
      }
    } else if (statement instanceof ListRolesStatement) {
      ListRolesStatement stmt = (ListRolesStatement) castStatement;
      String role = getRoleResourceFromStatement(stmt, "grantee");
      logger.debug(
          "preparing to authorize statement of type {} on {}",
          castStatement.getClass().toString(),
          role);

      try {
        authorization.authorizeRoleRead(authenticationSubject, role, SourceAPI.CQL);
      } catch (io.stargate.auth.UnauthorizedException e) {
        logger.debug("Unauthorized statement", e);
        throw new UnauthorizedException(
            String.format("Missing correct permission on role %s: %s", role, e.getMessage()));
      }

      logger.debug("authorized statement of type {} on {}", castStatement.getClass(), role);
    }
  }

  private void authorizeAlterSchemaStatement(
      CQLStatement statement,
      AuthenticationSubject authenticationSubject,
      AuthorizationService authorization) {
    AlterSchemaStatement castStatement = (AlterSchemaStatement) statement;
    Scope scope = null;
    ResourceKind resource = null;
    String keyspaceName = null;
    String tableName = null;

    if (statement instanceof CreateTableStatement) {
      scope = Scope.CREATE;
      resource = ResourceKind.TABLE;
      keyspaceName = castStatement.keyspace();
      tableName = ((CreateTableStatement) castStatement).table();
    } else if (statement instanceof DropTableStatement) {
      scope = Scope.DROP;
      resource = ResourceKind.TABLE;
      keyspaceName = castStatement.keyspace();
      tableName = ((DropTableStatement) castStatement).table();
    } else if (statement instanceof AlterTableStatement) {
      scope = Scope.ALTER;
      resource = ResourceKind.TABLE;
      keyspaceName = castStatement.keyspace();
      tableName = ((AlterTableStatement) castStatement).table();
    } else if (statement instanceof CreateKeyspaceStatement) {
      scope = Scope.CREATE;
      resource = ResourceKind.KEYSPACE;
      keyspaceName = castStatement.keyspace();
      tableName = null;
    } else if (statement instanceof DropKeyspaceStatement) {
      scope = Scope.DROP;
      resource = ResourceKind.KEYSPACE;
      keyspaceName = castStatement.keyspace();
      tableName = null;
    } else if (statement instanceof AlterKeyspaceStatement) {
      scope = Scope.ALTER;
      resource = ResourceKind.KEYSPACE;
      keyspaceName = castStatement.keyspace();
      tableName = null;
    } else if (statement instanceof AlterTypeStatement) {
      scope = Scope.ALTER;
      resource = ResourceKind.TYPE;
      keyspaceName = castStatement.keyspace();
    } else if (statement instanceof AlterViewStatement) {
      scope = Scope.ALTER;
      resource = ResourceKind.VIEW;
      keyspaceName = castStatement.keyspace();
      tableName = ((AlterViewStatement) castStatement).table();
    } else if (statement instanceof CreateAggregateStatement) {
      scope = Scope.CREATE;
      resource = ResourceKind.AGGREGATE;
      keyspaceName = castStatement.keyspace();
    } else if (statement instanceof CreateFunctionStatement) {
      scope = Scope.CREATE;
      resource = ResourceKind.FUNCTION;
      keyspaceName = castStatement.keyspace();
    } else if (statement instanceof CreateIndexStatement) {
      scope = Scope.CREATE;
      resource = ResourceKind.INDEX;
      keyspaceName = castStatement.keyspace();
      tableName = ((CreateIndexStatement) castStatement).table();
    } else if (statement instanceof CreateTriggerStatement) {
      scope = Scope.CREATE;
      resource = ResourceKind.TRIGGER;
      keyspaceName = castStatement.keyspace();
      tableName = ((CreateTriggerStatement) castStatement).table();
    } else if (statement instanceof CreateTypeStatement) {
      scope = Scope.CREATE;
      resource = ResourceKind.TYPE;
      keyspaceName = castStatement.keyspace();
    } else if (statement instanceof CreateViewStatement) {
      scope = Scope.CREATE;
      resource = ResourceKind.VIEW;
      keyspaceName = castStatement.keyspace();
      tableName = ((CreateViewStatement) castStatement).table();
    } else if (statement instanceof DropAggregateStatement) {
      scope = Scope.DROP;
      resource = ResourceKind.AGGREGATE;
      keyspaceName = castStatement.keyspace();
    } else if (statement instanceof DropFunctionStatement) {
      scope = Scope.DROP;
      resource = ResourceKind.FUNCTION;
      keyspaceName = castStatement.keyspace();
    } else if (statement instanceof DropIndexStatement) {
      scope = Scope.DROP;
      resource = ResourceKind.INDEX;
      keyspaceName = castStatement.keyspace();
      tableName = ((DropIndexStatement) castStatement).table();
    } else if (statement instanceof DropTriggerStatement) {
      scope = Scope.DROP;
      resource = ResourceKind.TRIGGER;
      keyspaceName = castStatement.keyspace();
      tableName = ((DropTriggerStatement) castStatement).table();
    } else if (statement instanceof DropTypeStatement) {
      scope = Scope.DROP;
      resource = ResourceKind.TYPE;
      keyspaceName = castStatement.keyspace();
    } else if (statement instanceof DropViewStatement) {
      scope = Scope.DROP;
      resource = ResourceKind.VIEW;
      keyspaceName = castStatement.keyspace();
      tableName = ((DropViewStatement) castStatement).table();
    }

    logger.debug(
        "preparing to authorize statement of type {} on {}.{}",
        castStatement.getClass().toString(),
        keyspaceName,
        tableName);

    try {
      authorization.authorizeSchemaWrite(
          authenticationSubject, keyspaceName, tableName, scope, SourceAPI.CQL, resource);
    } catch (io.stargate.auth.UnauthorizedException e) {
      String msg =
          String.format(
              "Missing correct permission on %s.%s",
              keyspaceName, (tableName == null ? "" : tableName));
      if (e.getMessage() != null) {
        msg += ": " + e.getMessage();
      }
      throw new UnauthorizedException(msg);
    }

    logger.debug(
        "authorized statement of type {} on {}.{}",
        castStatement.getClass(),
        keyspaceName,
        tableName);
  }

  private String getRoleResourceFromStatement(Object stmt, String fieldName) {
    try {
      Class<?> aClass = stmt.getClass();
      if (stmt instanceof ListUsersStatement
          || stmt instanceof ListPermissionsStatement
          || stmt instanceof RevokeRoleStatement
          || stmt instanceof GrantRoleStatement) {
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
