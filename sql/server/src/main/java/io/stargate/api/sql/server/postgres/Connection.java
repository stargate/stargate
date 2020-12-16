/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.api.sql.server.postgres;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.Scheduler;
import io.reactivex.Single;
import io.reactivex.schedulers.Schedulers;
import io.reactivex.subjects.PublishSubject;
import io.stargate.api.sql.server.postgres.msg.AuthenticationOk;
import io.stargate.api.sql.server.postgres.msg.Bind;
import io.stargate.api.sql.server.postgres.msg.BindComplete;
import io.stargate.api.sql.server.postgres.msg.EncryptionResponse;
import io.stargate.api.sql.server.postgres.msg.ErrorResponse;
import io.stargate.api.sql.server.postgres.msg.Execute;
import io.stargate.api.sql.server.postgres.msg.ExtendedQueryMessage;
import io.stargate.api.sql.server.postgres.msg.NoticeResponse;
import io.stargate.api.sql.server.postgres.msg.PGClientMessage;
import io.stargate.api.sql.server.postgres.msg.PGServerMessage;
import io.stargate.api.sql.server.postgres.msg.ParameterStatus;
import io.stargate.api.sql.server.postgres.msg.Parse;
import io.stargate.api.sql.server.postgres.msg.ParseComplete;
import io.stargate.api.sql.server.postgres.msg.Query;
import io.stargate.api.sql.server.postgres.msg.ReadyForQuery;
import io.stargate.api.sql.server.postgres.msg.StartupMessage;
import io.stargate.auth.AuthenticationService;
import io.stargate.db.datastore.DataStore;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** <a href="https://www.postgresql.org/docs/13/protocol-flow.html">PostgreSQL Message Flow</a>. */
public class Connection {

  private static final Logger LOG = LoggerFactory.getLogger(Connection.class);

  private static final int MAX_MESSAGES_BEFORE_FLUSH =
      Integer.getInteger("stargate.pgsql.max.message.before.flushg", 100);

  private static final AtomicInteger threadCount = new AtomicInteger();

  private final Channel channel;
  private final SqlParser parser;
  private final AuthenticationService authenticationService;

  private final ConcurrentMap<String, Statement> statements = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Portal> portals = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, String> properties = new ConcurrentHashMap<>();
  private final PublishSubject<PGClientMessage> publisher;
  private int messagesWritten;
  private Throwable error = null;

  public Connection(
      Channel channel, DataStore dataStore, AuthenticationService authenticationService) {
    this.channel = channel;
    this.parser = new SqlParser(dataStore);
    this.authenticationService = authenticationService;

    publisher = PublishSubject.create();

    // TODO: use async API with DataStore
    Scheduler executor =
        Schedulers.from(
            new ScheduledThreadPoolExecutor(
                4,
                runnable -> {
                  Thread thread = new Thread(runnable, "pg-exec-" + threadCount.getAndIncrement());
                  thread.setContextClassLoader(Connection.class.getClassLoader());
                  return thread;
                }));

    publisher
        .observeOn(executor) // Note: calls to Calcite methods may block
        .toFlowable(BackpressureStrategy.BUFFER)
        .concatMap(m -> m.dispatch(this))
        .doOnNext(
            msg -> {
              LOG.trace("write: {}", msg.getClass().getSimpleName());
              channel.write(msg);
              messagesWritten++;

              boolean shouldFlush = messagesWritten % MAX_MESSAGES_BEFORE_FLUSH == 0;
              if (shouldFlush || msg.flush()) {
                LOG.trace("flush: {}", msg.getClass().getSimpleName());
                channel.flush();
              }
            })
        .onErrorResumeNext(
            th -> {
              LOG.error(
                  "Unrecoverable error while processing a PostgreSQL message (closing connection): {}",
                  th.toString(),
                  th);
              channel
                  .writeAndFlush(ErrorResponse.fatal(th))
                  .addListener(ChannelFutureListener.CLOSE);
              return Flowable.empty();
            })
        .subscribe();
  }

  public Flowable<PGServerMessage> simpleQuery(Query message) {
    LOG.trace("simple: {}", message.getClass().getSimpleName());
    return Flowable.just(message)
        .concatMap(this::execute)
        // Note: we do not "remember" errors in the simple query sub-protocol
        .onErrorResumeNext(this::toErrorMessage)
        .concatWith(Flowable.just(ReadyForQuery.instance()));
  }

  public Flowable<PGServerMessage> extendedQuery(ExtendedQueryMessage message) {
    LOG.trace("dispatching extended: {}", message.getClass().getSimpleName());
    return Flowable.just(message)
        .concatMap(
            msg -> {
              if (error != null) {
                LOG.trace("skipped: {}", msg.getClass().getSimpleName());
                return Flowable.empty();
              }

              return msg.process(this);
            })
        .onErrorResumeNext(
            th -> {
              error = th;
              return toErrorMessage(th);
            });
  }

  public void enqueue(PGClientMessage msg) {
    LOG.trace("enqueue: {}", msg.getClass().getSimpleName());
    publisher.onNext(msg);
  }

  private Flowable<PGServerMessage> toErrorMessage(Throwable th) {
    LOG.error("Error while processing a PostgreSQL message: {}", th.toString(), th);
    return Flowable.just(ErrorResponse.error(th));
  }

  public Flowable<PGServerMessage> handshake(StartupMessage message) {
    if (message.isGssRequest() || message.isSslRequest()) {
      return Flowable.just(new EncryptionResponse(false));
    }

    if (!message.startupRequest()) {
      throw new IllegalStateException("Unexpected startup message");
    }

    return Flowable.just(
        new AuthenticationOk(),
        ParameterStatus.serverVersion("13.0"),
        ParameterStatus.serverEncoding(),
        ParameterStatus.timeZone(),
        ParameterStatus.clientEncoding(),
        NoticeResponse.warning("PostgreSQL protocol support is experimental in Stargate"),
        ReadyForQuery.instance());
  }

  public void setProperty(String key, String value) {
    LOG.trace("set ({}): {} = {}", channel.remoteAddress(), key, value);
    properties.put(key, value);
  }

  public Flowable<PGServerMessage> prepare(Parse message) {
    statements.compute(message.getName(), (k, v) -> prepareInternal(message.getSql()));
    return Flowable.just(ParseComplete.instance());
  }

  private Statement prepareInternal(String sql) {
    return parser.parse(sql);
  }

  public Flowable<PGServerMessage> bind(Bind message) {
    portals.compute(message.getPortalName(), (k, v) -> bindInternal(message));
    return Flowable.just(BindComplete.instance());
  }

  private Portal bindInternal(Bind message) {
    String statementName = message.getStatementName();

    Statement statement =
        statements.computeIfAbsent(
            statementName,
            n -> {
              throw new IllegalStateException("Unknown statement: " + statementName);
            });

    return statement.bind(message);
  }

  private Flowable<PGServerMessage> execute(Query message) {
    return Flowable.fromIterable(sqlCommands(message)).concatMap(this::executeSimple);
  }

  private List<String> sqlCommands(Query message) {
    String commandBlock = message.getSql();
    String[] commands = commandBlock.split(";"); // TODO: handle escapes
    return Arrays.asList(commands);
  }

  private Flowable<PGServerMessage> executeSimple(String sql) {
    Statement statement = prepareInternal(sql);
    Portal portal = statement.bind(Bind.empty());
    return portal.describeSimple().concatWith(portal.execute(this));
  }

  public Flowable<PGServerMessage> execute(Execute message) {
    String portalName = message.getPortalName();
    Portal portal =
        portals.computeIfAbsent(
            portalName,
            n -> {
              throw new IllegalStateException("Unknown portal: " + portalName);
            });

    return portal.execute(this);
  }

  public Single<PGServerMessage> sync() {
    error = null;
    return Single.just(ReadyForQuery.instance());
  }

  public void flush() {
    channel.flush();
  }

  public void terminate() {
    channel.close();
  }

  public Flowable<PGServerMessage> describePortal(String name) {
    Portal portal = portals.get(name);
    if (portal == null) {
      throw new IllegalArgumentException("Unknown portal: '" + name + "'");
    }

    return Flowable.just(portal.describe());
  }
}
