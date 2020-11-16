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
import io.netty.channel.ChannelHandlerContext;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.Scheduler;
import io.reactivex.Single;
import io.reactivex.SingleEmitter;
import io.reactivex.SingleOnSubscribe;
import io.reactivex.annotations.NonNull;
import io.reactivex.schedulers.Schedulers;
import io.reactivex.subjects.PublishSubject;
import io.stargate.api.sql.server.postgres.msg.AuthenticationOk;
import io.stargate.api.sql.server.postgres.msg.Bind;
import io.stargate.api.sql.server.postgres.msg.BindComplete;
import io.stargate.api.sql.server.postgres.msg.EncryptionResponse;
import io.stargate.api.sql.server.postgres.msg.ErrorResponse;
import io.stargate.api.sql.server.postgres.msg.Execute;
import io.stargate.api.sql.server.postgres.msg.NoticeResponse;
import io.stargate.api.sql.server.postgres.msg.PGClientMessage;
import io.stargate.api.sql.server.postgres.msg.PGServerMessage;
import io.stargate.api.sql.server.postgres.msg.ParameterStatus;
import io.stargate.api.sql.server.postgres.msg.Parse;
import io.stargate.api.sql.server.postgres.msg.ParseComplete;
import io.stargate.api.sql.server.postgres.msg.ReadyForQuery;
import io.stargate.api.sql.server.postgres.msg.RowDescription;
import io.stargate.api.sql.server.postgres.msg.StartupMessage;
import io.stargate.auth.AuthenticationService;
import io.stargate.db.datastore.DataStore;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** <a href="https://www.postgresql.org/docs/13/protocol-flow.html">PostgreSQL Message Flow</a>. */
public class Connection {

  private static final Logger LOG = LoggerFactory.getLogger(Connection.class);

  private final Channel channel;
  private final DataStore dataStore;
  private final SqlParser parser;
  private final AuthenticationService authenticationService;
  // TODO: use async API with DataStore
  private final ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(4);
  private final Scheduler scheduler = Schedulers.from(executor);

  private final ConcurrentMap<String, Statement> statements = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Portal> portals = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, String> properties = new ConcurrentHashMap<>();
  private final PublishSubject<PGClientMessage> publisher;
  private Throwable error = null;

  public Connection(
      Channel channel, DataStore dataStore, AuthenticationService authenticationService) {
    this.channel = channel;
    this.dataStore = dataStore;
    this.parser = new SqlParser(dataStore);
    this.authenticationService = authenticationService;

    ChannelHandlerContext ctx = channel.pipeline().lastContext();
    publisher = PublishSubject.create();
    Scheduler channelScheduler = Schedulers.from(channel.eventLoop());
    publisher
        .observeOn(channelScheduler)
        .toFlowable(BackpressureStrategy.BUFFER)
        .concatMap(this::process)
        .doOnNext(
            msg -> {
              LOG.info("write: " + msg.getClass().getSimpleName());
              channel.write(msg);
              if (msg.flush()) {
                LOG.info("flush: " + msg.getClass().getSimpleName());
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

  public void enqueue(PGClientMessage msg) {
    LOG.info("enqueue: " + msg.getClass().getSimpleName());
    publisher.onNext(msg);
  }

  public boolean hasErrors() {
    return error != null;
  }

  private Flowable<PGServerMessage> toErrorMessage(Throwable th) {
    LOG.error(
        "Error while processing a PostgreSQL message (closing connection): {}", th.toString(), th);
    error = th;
    return Flowable.just(ErrorResponse.error(th));
  }

  private Flowable<PGServerMessage> process(PGClientMessage message) {
    try {
      LOG.info("dispatching: " + message.getClass().getSimpleName());
      return message.process(this).onErrorResumeNext(this::toErrorMessage);
    } catch (Exception e) {
      return toErrorMessage(e);
    }
  }

  private <T> Single<T> exec(T value) {
    return Single.create(
        new SingleOnSubscribe<T>() {
          @Override
          public void subscribe(@NonNull SingleEmitter<T> emitter) throws Exception {
            //        LOG.info("exec1: " + value.getClass().getSimpleName());

            ScheduledFuture<?> future =
                executor.schedule(
                    () -> {
                      //          LOG.info("exec2: " + value.getClass().getSimpleName());
                      emitter.onSuccess(value);
                    },
                    20,
                    TimeUnit.MILLISECONDS);

            emitter.setCancellable(() -> future.cancel(true));
          }
        });
  }

  public Flowable<PGServerMessage> handshake(StartupMessage message) {
    return exec(message)
        .toFlowable()
        .concatMap(
            msg -> {
              if (msg.isGssRequest() || msg.isSslRequest()) {
                return Flowable.just(new EncryptionResponse(false));
              }

              if (!msg.startupRequest()) {
                throw new IllegalStateException("Unexpected startup message");
              }

              return Flowable.just(
                  new AuthenticationOk(),
                  ParameterStatus.serverVersion("13.0"),
                  NoticeResponse.warning("PostgreSQL protocol support is experimental in Stargate"),
                  ReadyForQuery.instance());
            });
  }

  public void setProperty(String key, String value) {
    LOG.info("set ({}): {} = {}", channel.remoteAddress(), key, value);
    properties.put(key, value);
  }

  public Single<PGServerMessage> prepare(Parse message) {
    return exec(message)
        .map(msg -> statements.compute(msg.getName(), (k, v) -> prepareInternal(msg)))
        .map(__ -> ParseComplete.instance());
  }

  private Statement prepareInternal(Parse message) {
    String sql = message.getSql().trim();
    return parser.parse(sql);
  }

  public Single<PGServerMessage> bind(Bind message) {
    return exec(message)
        .map(msg -> portals.compute(message.getPortalName(), (k, v) -> bindInternal(msg)))
        .map(__ -> BindComplete.instance());
  }

  private Portal bindInternal(Bind message) {
    String statementName = message.getStatementName();

    Statement statement =
        statements.computeIfAbsent(
            statementName,
            n -> {
              throw new IllegalStateException("Unknown statement: " + statementName);
            });

    return new Portal(statement, message.getResultFormatCodes());
  }

  public Flowable<PGServerMessage> execute(Execute message) {
    return exec(message)
        .flatMapPublisher(
            msg -> {
              String portalName = msg.getPortalName();
              Portal portal =
                  portals.computeIfAbsent(
                      portalName,
                      n -> {
                        throw new IllegalStateException("Unknown portal: " + portalName);
                      });

              return portal.execute(this);
            });
  }

  public Single<PGServerMessage> sync() {
    error = null;
    return Single.just(ReadyForQuery.instance());
  }

  public void flush() {
    channel.flush();
  }

  public Flowable<PGServerMessage> describePortal(String name) {
    Portal portal = portals.get(name);
    if (portal == null) {
      throw new IllegalArgumentException("Unknown portal: '" + name + "'");
    }

    return Flowable.just(RowDescription.from(portal));
  }
}
